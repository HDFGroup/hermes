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

#include "prefetcher.h"
#include "hermes.h"

namespace hermes {

/** Initialize each candidate prefetcher, including trace info */
void Prefetcher::Init() {
  mdm_ = &HERMES->mdm_;
  rpc_ = &HERMES->rpc_;
  borg_ = &HERMES->borg_;
  auto conf = HERMES->server_config_;

  // Make sure that prefetcher is enabled
  is_enabled_ = conf.prefetcher_.enabled_;
  mdm_->enable_io_tracing_ = is_enabled_;
  if (!is_enabled_) {
    return;
  }

  // Create the binary log
  /*if (conf.prefetcher_.trace_path_.empty()) {
    log_.Init("", MEGABYTES(64));
  } else {
    log_.Init(conf.prefetcher_.trace_path_ + )
  }*/

  // Info needed per-client and server
  mdm_->is_mpi_ = conf.prefetcher_.is_mpi_;
  if (HERMES->mode_ == HermesType::kClient) {
    // NOTE(llogan): prefetcher runs only on daemon as thread
    return;
  }

  // Set the epoch
  epoch_ms_ = (double)conf.prefetcher_.epoch_ms_;

  // Spawn the prefetcher thread
  auto prefetcher = [](void *args) {
    HILOG(kDebug, "Prefetcher has started")
    (void) args;
    Prefetcher *prefetch = &HERMES->prefetch_;
    while (HERMES_THREAD_MANAGER->Alive()) {
      prefetch->Run();
      tl::thread::self().sleep(*HERMES->rpc_.server_engine_,
                               prefetch->epoch_ms_);
    }
    HILOG(kDebug, "Prefetcher has stopped")
  };
  HERMES_THREAD_MANAGER->Spawn(prefetcher);
}

/** Finalize the prefetcher thread */
void Prefetcher::Finalize()  {
}

/** Parse the MDM's I/O pattern log */
void Prefetcher::Run() {
  // Ingest the current I/O statistics

  // Get the set of buckets
}

}  // namespace hermes
