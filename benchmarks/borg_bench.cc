/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Distributed under BSD 3-Clause license.                                   *
 * Copyright by The HDF Group.                                               *
 * Copyright by the Illinois Institute of Technology.                        *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of Hermes. The full Hermes copyright notice, including  *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the top directory. If you do not  *
 * have access to the file, you may request a copy from help@hdfgroup.org.   *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <numeric>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"
#include "metadata_management_internal.h"
#include "test_utils.h"

namespace hapi = hermes::api;
using HermesPtr = std::shared_ptr<hapi::Hermes>;

const int kDefaultIters = 2000;
const size_t kDefaultBlobSize = KILOBYTES(32);

struct Options {
  bool use_borg;
  bool verify;
  bool time_puts;
  bool verbose;
  bool debug;
  bool write_only;
  bool mixed;
  long sleep_ms;
  size_t blob_size;
  int iters;
  char *output_filename;
};

static void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s [-bdmpvwx] [-f <string>]\n", program);
  fprintf(stderr, "  -b\n");
  fprintf(stderr, "    Enable the BORG for the write-only case.\n");
  fprintf(stderr, "  -d\n");
  fprintf(stderr, "    If present, enable MPI breakpoint for debugging.\n");
  fprintf(stderr, "  -f\n");
  fprintf(stderr, "    The filename of the persisted data (for correctness"
          " verification).\n");
  fprintf(stderr, "  -i\n");
  fprintf(stderr, "    Number of iterations (default: %d)\n", kDefaultIters);
  fprintf(stderr, "  -m\n");
  fprintf(stderr, "    Run mixed workload.\n");
  fprintf(stderr, "  -p\n");
  fprintf(stderr, "    Get average for groups of puts.\n");
  fprintf(stderr, "  -s\n");
  fprintf(stderr, "    Sleep ms between each Put.\n");
  fprintf(stderr, "  -v\n");
  fprintf(stderr, "    Print verbose information.\n");
  fprintf(stderr, "  -w\n");
  fprintf(stderr, "    Run write only workload.\n");
  fprintf(stderr, "  -x\n");
  fprintf(stderr, "    If present, verify results at the end.\n");
  fprintf(stderr, "  -z\n");
  fprintf(stderr, "    Blob size in bytes (default: %zu).\n", kDefaultBlobSize);
}

static Options HandleArgs(int argc, char **argv) {
  Options result = {};
  result.iters = kDefaultIters;
  result.blob_size = kDefaultBlobSize;

  int option = -1;
  while ((option = getopt(argc, argv, "bdf:hi:mps:vxwz:")) != -1) {
    switch (option) {
      case 'h': {
        PrintUsage(argv[0]);
        exit(0);
      }
      case 'b': {
        result.use_borg = true;
        break;
      }
      case 'd': {
        result.debug = true;
        break;
      }
      case 'f': {
        result.output_filename = optarg;
        break;
      }
      case 'i': {
        result.iters = strtol(optarg, NULL, 0);
        break;
      }
      case 'm': {
        result.mixed = true;
        break;
      }
      case 'p': {
        result.time_puts = true;
        break;
      }
      case 's': {
        result.sleep_ms = strtol(optarg, NULL, 0);
        break;
      }
      case 'v': {
        result.verbose  = true;
        break;
      }
      case 'x': {
        result.verify  = true;
        break;
      }
      case 'w': {
        result.write_only = true;
        break;
      }
      case 'z': {
        result.blob_size = strtoll(optarg, NULL, 0);
        break;
      }
      default: {
        PrintUsage(argv[0]);
        exit(1);
      }
    }
  }

  if (result.verify && !result.output_filename) {
    fprintf(stderr, "Please supply filename via -f\n");
    exit(1);
  }
  if (optind < argc) {
    fprintf(stderr, "non-option ARGV-elements: ");
    while (optind < argc) {
      fprintf(stderr, "%s ", argv[optind++]);
    }
    fprintf(stderr, "\n");
  }
  return result;
}

static double GetMPIAverage(double rank_seconds, int num_ranks, MPI_Comm comm) {
  double total_secs = 0;
  MPI_Reduce(&rank_seconds, &total_secs, 1, MPI_DOUBLE, MPI_SUM, 0, comm);
  double result = total_secs / num_ranks;

  return result;
}

static double GetBandwidth(double total_elapsed, double total_mb, MPI_Comm comm,
                    int ranks) {
  double avg_total_seconds = GetMPIAverage(total_elapsed, ranks, comm);
  double result = total_mb / avg_total_seconds;

  return result;
}

static std::string MakeBlobName(int rank, int i) {
  std::string result = std::to_string(rank) + "_" + std::to_string(i);

  return result;
}

static void WriteOnlyWorkload(const Options &options) {
  HermesPtr hermes = hapi::InitHermes(getenv("HERMES_CONF"));

  if (hermes->IsApplicationCore()) {
    int rank = hermes->GetProcessRank();
    const int kNumRanks = hermes->GetNumProcesses();
    const size_t kTotalBytes = kNumRanks * options.blob_size * options.iters;

    hermes::testing::Timer timer;
    hapi::Context ctx;
    // Disable swapping of Blobs
    ctx.disable_swap = true;
    ctx.policy = hapi::PlacementPolicy::kMinimizeIoTime;

    std::string bkt_name = "BORG_" + std::to_string(rank);
    hapi::VBucket vbkt(bkt_name, hermes);
    hapi::Bucket bkt(bkt_name, hermes, ctx);

    hapi::WriteOnlyTrait trait;
    if (options.use_borg) {
      vbkt.Attach(&trait);
    }

    // MinIoTime with retry
    const int kReportFrequency = 30;
    hermes::testing::Timer put_timer;
    size_t failed_puts = 0;
    size_t failed_links = 0;
    size_t retries = 0;
    for (int i = 0; i < options.iters; ++i) {
      std::string blob_name = MakeBlobName(rank, i);
      hapi::Blob blob(options.blob_size, i % 255);

      timer.resumeTime();
      put_timer.resumeTime();
      hapi::Status status;
      int consecutive_fails = 0;
      while (!((status = bkt.Put(blob_name, blob)).Succeeded())) {
        retries++;
        if (++consecutive_fails > 10) {
          failed_puts++;
          break;
        }
      }

      if (options.use_borg && consecutive_fails <= 10) {
        hapi::Status link_status = vbkt.Link(blob_name, bkt_name);
        if (!link_status.Succeeded()) {
          failed_links++;
        }
      }

      if (options.sleep_ms > 0 && i > 0 && i % kReportFrequency == 0) {
        std::this_thread::sleep_for(
          std::chrono::milliseconds(options.sleep_ms));
      }

      put_timer.pauseTime();
      timer.pauseTime();

      if (options.time_puts && i > 0 && i % kReportFrequency == 0) {
        Assert(kNumRanks == 1);
        double total_mb =
          (options.blob_size * kReportFrequency) / 1024.0 / 1024.0;

        std::cout << i << ", " << total_mb / put_timer.getElapsedTime() << "\n";
      }
      hermes->AppBarrier();
    }

    Assert(failed_puts == 0);
    if (options.verbose) {
      std::cout << "Rank " << rank << " failed puts: " << failed_puts << "\n";
      std::cout << "Rank " << rank << " failed links: " << failed_links << "\n";
      std::cout << "Rank " << rank << " Put retries: " << retries << "\n";
    }

    hermes->AppBarrier();
    if (!hermes->IsFirstRankOnNode()) {
      vbkt.Release();
      bkt.Release();
    }

    hermes->AppBarrier();
    if (hermes->IsFirstRankOnNode()) {
      vbkt.Destroy();
      if (options.verify) {
        hapi::VBucket file_vbucket(options.output_filename, hermes);
        auto offset_map = std::unordered_map<std::string, hermes::u64>();

        for (int i = 0; i < kNumRanks; ++i) {
          for (int j = 0; j < options.iters; ++j) {
            std::string blob_name = MakeBlobName(i, j);
            file_vbucket.Link(blob_name, bkt_name, ctx);
            const size_t kBytesPerRank = options.iters * options.blob_size;
            size_t offset = (i * kBytesPerRank) + (j * options.blob_size);
            offset_map.emplace(blob_name, offset);
          }
        }
        bool flush_synchronously = true;
        hapi::PersistTrait persist_trait(options.output_filename, offset_map,
                                         flush_synchronously);
        if (options.verbose) {
          std::cout << "Flushing buffers...\n";
        }
        file_vbucket.Attach(&persist_trait);

        file_vbucket.Destroy();
      }
      bkt.Destroy();
    }

    hermes->AppBarrier();

    MPI_Comm *comm = (MPI_Comm *)hermes->GetAppCommunicator();
    double total_mb = kTotalBytes / 1024.0 / 1024.0;
    double bandwidth = GetBandwidth(timer.getElapsedTime(), total_mb, *comm,
                                    kNumRanks);

    if (hermes->IsFirstRankOnNode()) {
      std::cout << bandwidth << "," << kNumRanks << "," << options.use_borg
                << "," << options.sleep_ms << "\n";
    }
  }

  hermes->Finalize();
}

static void OptimizeReads(const Options &options) {
  HermesPtr hermes = hapi::InitHermes(getenv("HERMES_CONF"));

  if (hermes->IsApplicationCore()) {
    // Optimize reads
    // Fill hierarchy
    // Delete all RAM Blobs
    // BORG moves BB Blobs to RAM
    // Read all BB Blobs at RAM BW

    using namespace hermes;  // NOLINT(*)

    int rank = hermes->GetProcessRank();
    const int kNumRanks = hermes->GetNumProcesses();
    // const size_t kTotalBytes = kNumRanks * options.blob_size * options.iters;
    MetadataManager *mdm = GetMetadataManagerFromContext(&hermes->context_);
    std::vector<TargetID> targets(mdm->node_targets.length);

    for (u16 i = 0; i < mdm->node_targets.length; ++i) {
      targets[i] = {1, i, i};
    }

    GlobalSystemViewState *gsvs = GetGlobalSystemViewState(&hermes->context_);
    f32 ram_min_threshold = gsvs->bo_capacity_thresholds[0].min;
    f32 nvme_min_threshold = gsvs->bo_capacity_thresholds[1].min;

    std::vector<u64> capacities =
      GetRemainingTargetCapacities(&hermes->context_, &hermes->rpc_, targets);

    // See how many blobs we can fit in each Target
    std::vector<int> blobs_per_target(capacities.size());
    for (size_t i = 0; i < blobs_per_target.size(); ++i) {
      blobs_per_target[i] = capacities[i] / options.blob_size;
    }

    hermes::testing::Timer timer;
    hapi::Context ctx;
    // Disable swapping of Blobs
    ctx.disable_swap = true;
    ctx.policy = hapi::PlacementPolicy::kMinimizeIoTime;

    std::string bkt_name = __func__ + std::to_string(rank);
    hapi::Bucket bkt(bkt_name, hermes, ctx);

    // MinIoTime with retry
    // const int kReportFrequency = 30;
    hermes::testing::Timer put_timer;
    size_t failed_puts = 0;
    size_t retries = 0;

    // Fill hierarchy
    for (size_t target_idx = 0; target_idx < blobs_per_target.size();
         ++target_idx) {
      for (int i = 0; i < blobs_per_target[target_idx]; ++i) {
        std::string blob_name = (std::to_string(rank) + "_"
                                 + std::to_string(target_idx) + "_"
                                 + std::to_string(i));
        hapi::Blob blob(options.blob_size, i % 255);

        hapi::Status status;
        int consecutive_fails = 0;

        while (!((status = bkt.Put(blob_name, blob)).Succeeded())) {
          retries++;
          if (++consecutive_fails > 10) {
            failed_puts++;
            break;
          }
        }
      }
      hermes->AppBarrier();
    }

    Assert(failed_puts == 0);
    if (options.verbose) {
      std::cout << "Rank " << rank << " failed puts: " << failed_puts << "\n";
      std::cout << "Rank " << rank << " Put retries: " << retries << "\n";
    }

    // Delete all RAM and NVMe Blobs
    for (size_t j = 0; j < blobs_per_target.size() - 1; ++j) {
      for (int i = 0; i < blobs_per_target[j]; ++i) {
        std::string blob_name = (std::to_string(rank) + "_"
                                 + std::to_string(j) + "_"
                                 + std::to_string(i));
        Assert(bkt.DeleteBlob(blob_name).Succeeded());
      }
    }

    // Give the BORG time to move BB Blobs to RAM and NVMe
    std::this_thread::sleep_for(std::chrono::seconds(3));

    // Read all BB Blobs at RAM and NVMe BW
    const int kBbIndex = 2;

    int blobs_to_read = blobs_per_target[kBbIndex];
    if (ram_min_threshold > 0) {
      blobs_to_read = (ram_min_threshold * blobs_per_target[0] +
                       nvme_min_threshold * blobs_per_target[1]);
    }
    int stopping_index = blobs_per_target[kBbIndex] - blobs_to_read;
    for (int i = blobs_per_target[kBbIndex] - 1; i > stopping_index; --i) {
      std::string blob_name = (std::to_string(rank) + "_"
                               + std::to_string(kBbIndex) + "_"
                               + std::to_string(i));

      hapi::Blob blob(options.blob_size);
      timer.resumeTime();
      Assert(bkt.Get(blob_name, blob) == options.blob_size);
      timer.pauseTime();

      // Verify
      hapi::Blob expected_blob(options.blob_size, i % 255);
      Assert(blob == expected_blob);
    }

    if (!hermes->IsFirstRankOnNode()) {
      bkt.Release();
    }
    hermes->AppBarrier();
    if (hermes->IsFirstRankOnNode()) {
      bkt.Destroy();
    }
    hermes->AppBarrier();

    MPI_Comm *comm = (MPI_Comm *)hermes->GetAppCommunicator();
    size_t bytes_read = blobs_per_target[kBbIndex] * options.blob_size;
    double total_mb = bytes_read / 1024.0 / 1024.0;
    double bandwidth = GetBandwidth(timer.getElapsedTime(), total_mb, *comm,
                                    kNumRanks);

    if (hermes->IsFirstRankOnNode()) {
      std::cout << bandwidth << "," << kNumRanks << "," << options.use_borg
                << "," << options.sleep_ms << "\n";
    }
  }

  hermes->Finalize();
}

static void Verify(const Options &options) {
  int my_rank;
  int comm_size;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

  const size_t kAppCores = comm_size - 1;
  const size_t kTotalBytes = kAppCores * options.iters * options.blob_size;
  if (my_rank == 0) {
    std::vector<hermes::u8> data(kTotalBytes);

    if (options.verbose) {
      std::cout << "Verifying data\n";
    }

    FILE *f = fopen(options.output_filename, "r");
    Assert(f);
    Assert(fseek(f, 0L, SEEK_END) == 0);
    size_t file_size = ftell(f);
    Assert(file_size == kTotalBytes);
    Assert(fseek(f, 0L, SEEK_SET) == 0);
    size_t result = fread(data.data(), kTotalBytes, 1, f);
    Assert(result == 1);

    for (size_t rank = 0; rank < kAppCores; ++rank) {
      for (int iter = 0; iter < options.iters; ++iter) {
        for (size_t byte = 0; byte < options.blob_size; ++byte) {
          Assert(data[(rank * options.iters * options.blob_size) +
                      (iter * options.blob_size) + byte] == iter % 255);
        }
      }
    }
  }
}

static void DebugBreak() {
  int gdb_iii = 0;
  char gdb_DEBUG_hostname[256];
  gethostname(gdb_DEBUG_hostname, sizeof(gdb_DEBUG_hostname));
  printf("PID %d on %s ready for attach\n", getpid(), gdb_DEBUG_hostname);
  fflush(stdout);
  while (0 == gdb_iii)
    sleep(5);
}

int main(int argc, char *argv[]) {
  Options options = HandleArgs(argc, argv);

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  if (options.debug) {
    DebugBreak();
  }

  if (options.write_only) {
    WriteOnlyWorkload(options);
  }
  if (options.mixed) {
    OptimizeReads(options);
  }
  if (options.verify) {
    Verify(options);
  }

  MPI_Finalize();

  return 0;
}
