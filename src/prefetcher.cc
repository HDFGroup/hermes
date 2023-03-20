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
  auto conf = HERMES->server_config_;
  mdm_->enable_io_tracing_ = conf.prefetcher_.enabled_;
  epoch_ms_ = (double)conf.prefetcher_.epoch_ms_;
  if (!conf.prefetcher_.enabled_ ||
      conf.prefetcher_.trace_path_.size() == 0) {
    return;
  }
  if (HERMES->mode_ == HermesType::kClient) {
    // NOTE(llogan): Prefetcher is per-daemon.
    return;
  }

  // Parse the I/O trace YAML log
  try {
    YAML::Node io_trace = YAML::LoadFile(conf.prefetcher_.trace_path_);
    LOG(INFO) << "Parsing the I/O trace at: "
              << conf.prefetcher_.trace_path_ << std::endl;
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
      trace_.emplace_back(trace);
    }
  } catch (std::exception &e) {
    LOG(FATAL) << e.what() << std::endl;
  }

  // Spawn the prefetcher thread
  auto prefetcher = [](void *args) {
    LOG(INFO) << "Prefetcher has started" << std::endl;
    (void) args;
    Prefetcher *prefetch = &HERMES->prefetch_;
    while (!HERMES->rpc_.kill_requested_.load()) {
      prefetch->Run();
      tl::thread::self().sleep(*HERMES->rpc_.server_engine_,
                               prefetch->epoch_ms_);
    }
  };

  ABT_xstream_create(ABT_SCHED_NULL, &execution_stream_);
  ABT_thread_create_on_xstream(execution_stream_,
                               prefetcher, nullptr,
                               ABT_THREAD_ATTR_NULL, NULL);
}

/** Parse the MDM's I/O pattern log */
void Prefetcher::Run() {
  size_t log_size = mdm_->io_pattern_log_->size();
  auto trace_iter = trace_.begin();
  auto client_iter = mdm_->io_pattern_log_->begin();
  for (size_t i = 0; i < log_size; ++i) {
    hipc::Ref<IoStat> stat = (*client_iter);
    IoTrace &trace_entry = *trace_iter;

    // TODO(llogan)

    ++client_iter;
    ++trace_iter;
  }
}

}  // namespace hermes