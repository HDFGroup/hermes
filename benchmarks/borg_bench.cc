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
#include "test_utils.h"

struct Options {
  bool use_borg;
  bool verify;
  bool time_puts;
  long sleep_ms;
  char *output_filename;
};

void PrintUsage(char *program) {
  fprintf(stderr, "Usage: %s [-b <bool>] [-f] <string>\n", program);
  fprintf(stderr, "  -b\n");
  fprintf(stderr, "    If present, enable the BORG.\n");
  fprintf(stderr, "  -f\n");
  fprintf(stderr, "    The filename of the persisted data (for correctness"
          "verification).\n");
  fprintf(stderr, "  -p\n");
  fprintf(stderr, "    Get average for groups of puts.\n");
  fprintf(stderr, "  -s\n");
  fprintf(stderr, "    Sleep ms between each Put.\n");
  fprintf(stderr, "  -v\n");
  fprintf(stderr, "    If present, verify results at the end.\n");
}

Options HandleArgs(int argc, char **argv) {
  Options result = {};
  int option = -1;
  while ((option = getopt(argc, argv, "bf:hps:v")) != -1) {
    switch (option) {
      case 'h': {
        PrintUsage(argv[0]);
        exit(0);
      }
      case 'b': {
        result.use_borg = true;
        break;
      }
      case 'f': {
        result.output_filename = optarg;
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
        result.verify  = true;
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


namespace hapi = hermes::api;
using HermesPtr = std::shared_ptr<hapi::Hermes>;

double GetMPIAverage(double rank_seconds, int num_ranks, MPI_Comm comm) {
  double total_secs = 0;
  MPI_Reduce(&rank_seconds, &total_secs, 1, MPI_DOUBLE, MPI_SUM, 0, comm);
  double result = total_secs / num_ranks;

  return result;
}

double GetBandwidth(double total_elapsed, double total_mb, MPI_Comm comm,
                    int ranks) {
  double avg_total_seconds = GetMPIAverage(total_elapsed, ranks, comm);
  double result = total_mb / avg_total_seconds;

  return result;
}

std::string MakeBlobName(int rank, int i) {
  std::string result = std::to_string(rank) + "_" + std::to_string(i);

  return result;
}

int main(int argc, char *argv[]) {
  const size_t kBlobSize = KILOBYTES(32);
  const int kIters = 2000;

  Options options = HandleArgs(argc, argv);

  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  // int gdb_iii = 0;
  // char gdb_DEBUG_hostname[256];
  // gethostname(gdb_DEBUG_hostname, sizeof(gdb_DEBUG_hostname));
  // printf("PID %d on %s ready for attach\n", getpid(), gdb_DEBUG_hostname);
  // fflush(stdout);
  // while (0 == gdb_iii)
  //   sleep(5);

  HermesPtr hermes = hapi::InitHermes(getenv("HERMES_CONF"));

  if (hermes->IsApplicationCore()) {
    int rank = hermes->GetProcessRank();
    const int kNumRanks = hermes->GetNumProcesses();
    const size_t kTotalBytes = kNumRanks * kBlobSize * kIters;

    hermes::testing::Timer timer;
    hapi::Context ctx;
    // Disable swapping of Blobs
    ctx.disable_swap = true;
    // ctx.policy = hapi::PlacementPolicy::kRoundRobin;

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
    for (int i = 0; i < kIters; ++i) {
      std::string blob_name = MakeBlobName(rank, i);
      hapi::Blob blob(kBlobSize, i % 255);

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
        // TODO(chogan): Support more than 1 rank
        Assert(kNumRanks == 1);
        constexpr double total_mb =
          (kBlobSize * kReportFrequency) / 1024.0 / 1024.0;

        std::cout << i << ", " << total_mb / put_timer.getElapsedTime()
                  << "\n";
        put_timer.reset();
      }
      hermes->AppBarrier();
    }

    Assert(failed_puts == 0);
    // std::cout << "Rank " << rank << " failed puts: " << failed_puts << "\n";
    // std::cout << "Rank " << rank << " failed links: " << failed_links << "\n";
    // std::cout << "Rank " << rank << " Put retries: " << retries << "\n";

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
          for (int j = 0; j < kIters; ++j) {
            std::string blob_name = MakeBlobName(i, j);
            file_vbucket.Link(blob_name, bkt_name, ctx);
            const size_t kBytesPerRank = kIters * kBlobSize;
            size_t offset = (i * kBytesPerRank) + (j * kBlobSize);
            offset_map.emplace(blob_name, offset);
          }
        }
        bool flush_synchronously = true;
        hapi::PersistTrait persist_trait(options.output_filename, offset_map,
                                         flush_synchronously);
        std::cout << "Flushing buffers...\n";
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

  int my_rank;
  int comm_size;
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

  const size_t kAppCores = comm_size - 1;
  const size_t kTotalBytes = kAppCores * kIters * kBlobSize;
  if (options.verify && my_rank == 0) {
    std::vector<hermes::u8> data(kTotalBytes);
    std::cout << "Verifying data\n";
    FILE *f = fopen(options.output_filename, "r");
    Assert(f);
    Assert(fseek(f, 0L, SEEK_END) == 0);
    size_t file_size = ftell(f);
    Assert(file_size == kTotalBytes);
    Assert(fseek(f, 0L, SEEK_SET) == 0);
    size_t result = fread(data.data(), kTotalBytes, 1, f);
    Assert(result == 1);

    for (size_t rank = 0; rank < kAppCores; ++rank) {
      for (size_t iter = 0; iter < kIters; ++iter) {
        for (size_t byte = 0; byte < kBlobSize; ++byte) {
          Assert(data[(rank * kIters * kBlobSize) + (iter * kBlobSize) + byte]
                 == iter % 255);
        }
      }
    }
  }

  MPI_Finalize();

  return 0;
}
