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
  mdm_ = HERMES->mdm_.get();
  rpc_ = &HERMES->rpc_;
  borg_ = HERMES->borg_.get();
  auto conf = HERMES->server_config_;

  // Make sure that prefetcher is enabled
  is_enabled_ = conf.prefetcher_.enabled_;
  mdm_->enable_io_tracing_ = is_enabled_;
  if (!is_enabled_) {
    return;
  }

  // Info needed per-client and server
  mdm_->is_mpi_ = conf.prefetcher_.is_mpi_;
  if (HERMES->mode_ == HermesType::kClient) {
    // NOTE(llogan): prefetcher runs only on daemon as thread
    return;
  }

  // Set the epoch
  epoch_ms_ = (double)conf.prefetcher_.epoch_ms_;

  // Parse the I/O trace YAML log
  try {
    if (conf.prefetcher_.trace_path_.size() == 0) {
      return;
    }
    YAML::Node io_trace = YAML::LoadFile(conf.prefetcher_.trace_path_);
    HILOG(kDebug, "Parsing the I/O trace at: {}",
          conf.prefetcher_.trace_path_)
    int nprocs;
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    trace_.resize(nprocs);
    for (YAML::Node log_entry : io_trace) {
      IoTrace trace;
      trace.node_id_ = log_entry[0].as<int>();
      if (trace.node_id_ != rpc_->node_id_) {
        continue;
      }
      trace.type_ = static_cast<IoType>(log_entry[1].as<int>());
      trace.blob_name_ = log_entry[2].as<std::string>();
      trace.tag_name_ = log_entry[3].as<std::string>();
      trace.blob_size_ = log_entry[4].as<size_t>();
      trace.organize_next_n_ = log_entry[5].as<int>();
      trace.score_ = log_entry[6].as<float>();
      trace.rank_ = log_entry[7].as<int>();
      trace_[trace.rank_].emplace_back(trace);
    }

    trace_off_.resize(nprocs);
    for (int i = 0; i < nprocs; ++i) {
      trace_off_[i] = trace_[i].begin();
    }
  } catch (std::exception &e) {
    HELOG(kFatal, e.what())
  }

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
  size_t log_size = mdm_->io_pattern_log_->size();
  // auto trace_iter = trace_.begin();
  auto client_iter = mdm_->io_pattern_log_->begin();
  if (log_size == 0) {
    return;
  }

  // Group I/O pattern log by rank
  int nprocs;
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  std::vector<std::list<IoStat>> patterns;
  patterns.resize(nprocs);
  for (size_t i = 0; i < log_size; ++i) {
    hipc::Ref<IoStat> stat = (*client_iter);
    int rank = stat->rank_;
    patterns[rank].emplace_back(*stat);
    ++client_iter;
  }

  // Analyze the per-rank prefetching decisions
  for (int i = 0; i < nprocs; ++i) {
    for (IoStat &stat : patterns[i]) {
      (void) stat;
      // We assume rank I/O is exactly the same as it was in the trace
      IoTrace &trace = *trace_off_[i];
      if (trace.organize_next_n_ == 0) {
        ++trace_off_[i];
        continue;
      }

      for (int j = 0; j < trace.organize_next_n_; ++j) {
        ++trace_off_[i];
        trace = *trace_off_[i];
        /*borg_->GlobalOrganizeBlob(trace.tag_name_,
                                  trace.blob_name_,
                                  trace.score_);*/
      }
      ++trace_off_[i];
      break;
    }
  }

  // Clear the log
  mdm_->ClearIoStats(log_size);
}

}  // namespace hermes
