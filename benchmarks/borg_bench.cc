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

double GetBandwidth(double total_elapsed, double total_mb, MPI_Comm comm, int ranks) {
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
  HermesPtr hermes = hapi::InitHermes(getenv("HERMES_CONF"));

  if (hermes->IsApplicationCore()) {
    hermes::testing::Timer timer;
    hapi::Context ctx;
    // Disable swapping of Blobs
    ctx.disable_swap = true;
    // disable MinimizeIoTime PlacementPolicy constraints
    ctx.minimize_io_time_options.minimum_remaining_capacity = 0;
    ctx.minimize_io_time_options.capacity_change_threshold = 0;

    std::string bkt_name = "BORG";
    hapi::VBucket vbkt(bkt_name, hermes);
    hapi::Bucket bkt(bkt_name, hermes);

    hapi::WriteOnlyTrait trait;
    vbkt.Attach(&trait);

    const size_t kBlobSize = KILOBYTES(4);
    hapi::Blob blob(kBlobSize);
    std::iota(blob.begin(), blob.end(), 0);

    // MinIoTime with retry
    const int kIters = 128;
    for (int i = 0; i < kIters; ++i) {
      std::string blob_name = "b" + std::to_string(i);
      timer.resumeTime();
      hapi::Status status;
      while (!status.Succeeded()) {
        status = bkt.Put(blob_name, blob);
      }
      if (use_borg) {
        vbkt.Link(blob_name, bkt_name);
      }
      timer.pauseTime();
      hermes->AppBarrier();
    }

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

    hermes->AppBarrier();
  }

  hermes->Finalize();

  MPI_Finalize();

  return 0;
}
