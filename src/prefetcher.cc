//
// Created by lukemartinlogan on 10/18/22.
//

#include "prefetcher.h"
#include "metadata_management.h"
#include "singleton.h"

using hermes::api::Hermes;

namespace hermes {

bool Prefetcher::LogIoStat(Hermes *hermes, IoLogEntry &entry) {
  RpcContext *rpc = &hermes->rpc_;
  u32 target_node = HashToNode(hermes, entry);
  bool result = false;
  if (target_node == rpc->node_id) {
    auto prefetcher = Singleton<Prefetcher>::GetInstance();
    prefetcher->Log(entry);
  } else {
    result = RpcCall<bool>(rpc, target_node, "LogIoStat", entry);
  }
  return result;
}

size_t Prefetcher::HashToNode(Hermes *hermes, IoLogEntry &entry) {
  CommunicationContext *comm = &hermes->comm_;
  size_t h1 = std::hash<u64>{}(entry.vbkt_id_.as_int);
  size_t h2 = std::hash<u64>{}(entry.bkt_id_.as_int);
  size_t hash = h1 ^ h2;
  return (hash % comm->num_nodes) + 1;
}

void Prefetcher::Log(IoLogEntry &entry) {
  lock_.lock();
  if (log_.size() == max_length_) {
    log_.pop_front();
  }
  log_.emplace_back(entry);
  lock_.unlock();
}

void Prefetcher::Process() {
}

}  // namespace hermes