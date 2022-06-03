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

#include <stdlib.h>

#include <numeric>

#include <mpi.h>

#include "hermes.h"
#include "bucket.h"
#include "vbucket.h"
#include "test_utils.h"

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

int main(int argc, char *argv[]) {
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return 1;
  }

  bool use_borg = true;

  if (argc == 2) {
    use_borg = false;
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
    hermes::testing::Timer timer;
    hapi::Context ctx;
    // Disable swapping of Blobs
    ctx.disable_swap = true;

    std::string bkt_name = "BORG_" + std::to_string(rank);
    hapi::VBucket vbkt(bkt_name, hermes);
    hapi::Bucket bkt(bkt_name, hermes, ctx);

    hapi::WriteOnlyTrait trait;
    if (use_borg) {
      vbkt.Attach(&trait);
    }

    const size_t kBlobSize = KILOBYTES(32);
    hapi::Blob blob(kBlobSize);
    std::iota(blob.begin(), blob.end(), 0);

    // MinIoTime with retry
    const int kIters = 2000;
    const int kReportFrequency = 30;
    hermes::testing::Timer put_timer;
    size_t failed_puts = 0;
    size_t failed_links = 0;
    for (int i = 0; i < kIters; ++i) {
      std::string blob_name = ("b_" + std::to_string(rank) + "_" +
                               std::to_string(i));
      timer.resumeTime();
      put_timer.resumeTime();
      hapi::Status status;
      int consecutive_fails = 0;
      while (!((status = bkt.Put(blob_name, blob)).Succeeded())) {
          failed_puts++;
          if (++consecutive_fails > 10) {
            break;
          }
      }
      put_timer.pauseTime();

      if (use_borg && consecutive_fails <= 10) {
        hapi::Status link_status = vbkt.Link(blob_name, bkt_name);
        if (!link_status.Succeeded()) {
          failed_links++;
        }
      }
      timer.pauseTime();
      if (i > 0 && i % kReportFrequency == 0) {
        // TODO(chogan): Support more than 1 rank
        constexpr double total_mb =
          (kBlobSize * kReportFrequency) / 1024.0 / 1024.0;

        std::cout << i << ", " << total_mb / put_timer.getElapsedTime() << "\n";
        put_timer.reset();
      }
      hermes->AppBarrier();
    }

    std::cout << "Rank " << rank << " failed puts: " << failed_puts << "\n";
    std::cout << "     " << "failed links: " << failed_links << "\n";

    hermes->AppBarrier();
    if (!hermes->IsFirstRankOnNode()) {
      vbkt.Release();
      bkt.Release();
    }

    hermes->AppBarrier();
    if (hermes->IsFirstRankOnNode()) {
      vbkt.Destroy();
      bkt.Destroy();
    }

    hermes->AppBarrier();

    MPI_Comm *comm = (MPI_Comm *)hermes->GetAppCommunicator();
    int num_ranks = hermes->GetNumProcesses();
    double total_mb = (kBlobSize * kIters * num_ranks) / 1024.0 / 1024.0;
    double bandwidth = GetBandwidth(timer.getElapsedTime(), total_mb, *comm,
                                    num_ranks);

    if (hermes->IsFirstRankOnNode()) {
      fprintf(stderr, "##################### %f MiB/s\n", bandwidth);
    }
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
