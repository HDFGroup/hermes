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

#include <sys/mman.h>

#include <cmath>

#include "glog/logging.h"

#include "utils.h"
#include "hermes.h"
#include "bucket.h"
#include "buffer_pool.h"
#include "buffer_pool_internal.h"
#include "metadata_management_internal.h"

namespace hermes {

std::vector<DeviceID> RoundRobinState::devices_;

namespace api {

int Context::default_buffer_organizer_retries;
PlacementPolicy Context::default_placement_policy;
bool Context::default_rr_split;

Status RenameBucket(const std::string &old_name,
                    const std::string &new_name,
                    Context &ctx) {
  (void)ctx;
  Status ret;

  LOG(INFO) << "Renaming Bucket from " << old_name << " to "
            << new_name << '\n';

  return ret;
}

Status TransferBlob(const Bucket &src_bkt,
                    const std::string &src_blob_name,
                    Bucket &dst_bkt,
                    const std::string &dst_blob_name,
                    Context &ctx) {
  (void)src_bkt;
  (void)dst_bkt;
  (void)ctx;
  Status ret;

  LOG(INFO) << "Transferring Blob from " << src_blob_name << " to "
            << dst_blob_name << '\n';

  return ret;
}

bool Hermes::IsApplicationCore() {
  bool result = comm_.proc_kind == ProcessKind::kApp;

  return result;
}

bool Hermes::IsFirstRankOnNode() {
  bool result = comm_.first_on_node;

  return result;
}

void Hermes::AppBarrier() {
  hermes::SubBarrier(&comm_);
}

bool Hermes::BucketContainsBlob(const std::string &bucket_name,
                                const std::string &blob_name) {
  BucketID bucket_id = GetBucketId(&context_, &rpc_, bucket_name.c_str());
  bool result = hermes::ContainsBlob(&context_, &rpc_, bucket_id, blob_name);

  return result;
}

bool Hermes::BucketExists(const std::string &bucket_name) {
  BucketID id = hermes::GetBucketId(&context_, &rpc_, bucket_name.c_str());
  bool result = !IsNullBucketId(id);

  return result;
}

int Hermes::GetProcessRank() {
  int result = comm_.sub_proc_id;

  return result;
}

int Hermes::GetNodeId() {
  int result = comm_.node_id;

  return result;
}

int Hermes::GetNumProcesses() {
  int result = comm_.app_size;

  return result;
}

void *Hermes::GetAppCommunicator() {
  void *result = hermes::GetAppCommunicator(&comm_);

  return result;
}

void Hermes::Finalize(bool force_rpc_shutdown) {
  hermes::Finalize(&context_, &comm_, &rpc_, shmem_name_.c_str(), &trans_arena_,
                   IsApplicationCore(), force_rpc_shutdown);
  is_initialized = false;
}

void Hermes::FinalizeClient(bool stop_daemon) {
  hermes::FinalizeClient(&context_, &rpc_, &comm_, &trans_arena_, stop_daemon);
}

void Hermes::RemoteFinalize() {
  hermes::RpcCall<void>(&rpc_, rpc_.node_id, "RemoteFinalize");
}

void Hermes::RunDaemon() {
  hermes::RunDaemon(&context_, &rpc_, &comm_, &trans_arena_,
                    shmem_name_.c_str());
}

}  // namespace api

ArenaInfo GetArenaInfo(Config *config) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  // NOTE(chogan): Assumes first Device is RAM
  size_t total_hermes_memory = RoundDownToMultiple(config->capacities[0],
                                                   page_size);
  size_t total_pages = total_hermes_memory / page_size;
  size_t pages_left = total_pages;

  ArenaInfo result = {};

  for (int i = kArenaType_Count - 1; i > kArenaType_BufferPool; --i) {
    size_t desired_pages =
      std::floor(config->arena_percentages[i] * total_pages);
    // NOTE(chogan): Each arena gets 1 page at minimum
    size_t pages = std::max(desired_pages, 1UL);
    pages_left -= pages;
    size_t num_bytes = pages * page_size;
    result.sizes[i] = num_bytes;
    result.total += num_bytes;
  }

  if (pages_left == 0) {
    // TODO(chogan): @errorhandling
    HERMES_NOT_IMPLEMENTED_YET;
  }

  // NOTE(chogan): BufferPool Arena gets remainder of pages
  result.sizes[kArenaType_BufferPool] = pages_left * page_size;
  result.total += result.sizes[kArenaType_BufferPool];

  return result;
}

SharedMemoryContext InitHermesCore(Config *config, CommunicationContext *comm,
                                   ArenaInfo *arena_info, Arena *arenas,
                                   RpcContext *rpc) {
  size_t shmem_size = (arena_info->total -
                       arena_info->sizes[kArenaType_Transient]);
  u8 *shmem_base = InitSharedMemory(config->buffer_pool_shmem_name, shmem_size);

  // NOTE(chogan): Initialize shared arenas
  ptrdiff_t base_offset = 0;
  for (int i = 0; i < kArenaType_Count; ++i) {
    if (i == kArenaType_Transient) {
      // NOTE(chogan): Transient arena exists per rank, not in shared memory
      continue;
    }
    size_t arena_size = arena_info->sizes[i];
    InitArena(&arenas[i], arena_size, shmem_base + base_offset);
    base_offset += arena_size;
  }

  SharedMemoryContext context = {};
  context.shm_base = shmem_base;
  context.shm_size = shmem_size;
  context.buffer_pool_offset = InitBufferPool(context.shm_base,
                                              &arenas[kArenaType_BufferPool],
                                              &arenas[kArenaType_Transient],
                                              comm->node_id, config);

  MetadataManager *mdm =
    PushClearedStruct<MetadataManager>(&arenas[kArenaType_MetaData]);
  context.metadata_manager_offset = (u8 *)mdm - (u8 *)shmem_base;

  rpc->state = CreateRpcState(&arenas[kArenaType_MetaData]);
  mdm->rpc_state_offset = (u8 *)rpc->state - shmem_base;

  rpc->host_numbers = PushArray<int>(&arenas[kArenaType_MetaData],
                                     config->host_numbers.size());
  for (size_t i = 0; i < config->host_numbers.size(); ++i) {
    rpc->host_numbers[i] = config->host_numbers[i];
  }
  mdm->host_numbers_offset = (u8 *)rpc->host_numbers - (u8 *)shmem_base;

  InitMetadataManager(mdm, &arenas[kArenaType_MetaData], config, comm->node_id);
  InitMetadataStorage(&context, mdm, &arenas[kArenaType_MetaData], config);

  ShmemClientInfo *client_info = (ShmemClientInfo *)shmem_base;
  client_info->mdm_offset = context.metadata_manager_offset;

  return context;
}

SharedMemoryContext
BootstrapSharedMemory(Arena *arenas, Config *config, CommunicationContext *comm,
                      RpcContext *rpc, bool is_daemon, bool is_adapter) {
  size_t bootstrap_size = KILOBYTES(4);
  u8 *bootstrap_memory = (u8 *)malloc(bootstrap_size);
  InitArena(&arenas[kArenaType_Transient], bootstrap_size, bootstrap_memory);

  ArenaInfo arena_info = GetArenaInfo(config);
  // NOTE(chogan): The buffering capacity for the RAM Device is the size of the
  // BufferPool Arena
  config->capacities[0] = arena_info.sizes[kArenaType_BufferPool];

  size_t trans_arena_size =
    InitCommunication(comm, &arenas[kArenaType_Transient],
                      arena_info.sizes[kArenaType_Transient], is_daemon,
                      is_adapter);

  GrowArena(&arenas[kArenaType_Transient], trans_arena_size);
  comm->state = arenas[kArenaType_Transient].base;

  InitRpcContext(rpc, comm->num_nodes, comm->node_id, config);

  SharedMemoryContext result = {};
  if (comm->proc_kind == ProcessKind::kHermes && comm->first_on_node) {
    result = InitHermesCore(config, comm, &arena_info, arenas, rpc);
  }

  return result;
}

// TODO(chogan): https://github.com/HDFGroup/hermes/issues/323
#if 0
static void InitGlog() {
  FLAGS_logtostderr = 1;
  const char kMinLogLevel[] = "GLOG_minloglevel";
  char *min_log_level = getenv(kMinLogLevel);

  if (!min_log_level) {
    FLAGS_minloglevel = 0;
  }

  FLAGS_v = 0;

  google::InitGoogleLogging("hermes");
}
#endif

std::shared_ptr<api::Hermes> InitHermes(Config *config, bool is_daemon,
                                        bool is_adapter) {
  // TODO(chogan): https://github.com/HDFGroup/hermes/issues/323
  // InitGlog();

  std::string base_shmem_name(config->buffer_pool_shmem_name);
  MakeFullShmemName(config->buffer_pool_shmem_name, base_shmem_name.c_str());

  // TODO(chogan): Do we need a transfer window arena? We can probably just use
  // the transient arena for this.
  Arena arenas[kArenaType_Count] = {};
  CommunicationContext comm = {};
  RpcContext rpc = {};
  SharedMemoryContext context =
    BootstrapSharedMemory(arenas, config, &comm, &rpc, is_daemon, is_adapter);

  WorldBarrier(&comm);
  std::shared_ptr<api::Hermes> result = nullptr;

  if (comm.proc_kind == ProcessKind::kHermes) {
    result = std::make_shared<api::Hermes>(context);
    result->shmem_name_ = std::string(config->buffer_pool_shmem_name);
  } else {
    context = GetSharedMemoryContext(config->buffer_pool_shmem_name);
    SubBarrier(&comm);
    result = std::make_shared<api::Hermes>(context);
    // NOTE(chogan): Give every App process a valid pointer to the internal RPC
    // state in shared memory
    MetadataManager *mdm = GetMetadataManagerFromContext(&context);
    rpc.state = (void *)(context.shm_base + mdm->rpc_state_offset);
    rpc.host_numbers =
      (int *)((u8 *)context.shm_base + mdm->host_numbers_offset);
  }

  InitFilesForBuffering(&context, comm);

  WorldBarrier(&comm);

  rpc.node_id = comm.node_id;
  rpc.num_nodes = comm.num_nodes;

  result->trans_arena_ = arenas[kArenaType_Transient];
  result->comm_ = comm;
  result->context_ = context;
  result->rpc_ = rpc;

  // NOTE(chogan): The RPC servers have to be started here because they need to
  // save a reference to the context and rpc instances that are members of the
  // Hermes instance.
  if (comm.proc_kind == ProcessKind::kHermes) {
    std::string host_number = GetHostNumberAsString(&result->rpc_,
                                                    result->rpc_.node_id);

    std::string rpc_server_addr = GetRpcAddress(config, host_number,
                                                config->rpc_port);
    result->rpc_.start_server(&result->context_, &result->rpc_,
                              &result->trans_arena_, rpc_server_addr.c_str(),
                              config->rpc_num_threads);

    std::string bo_address = GetRpcAddress(config, host_number,
                                           config->buffer_organizer_port);
    StartBufferOrganizer(&result->context_, &result->rpc_,
                         &result->trans_arena_, bo_address.c_str(),
                         config->bo_num_threads, config->buffer_organizer_port);

    double sleep_ms = config->system_view_state_update_interval_ms;
    StartGlobalSystemViewStateUpdateThread(&result->context_, &result->rpc_,
                                           &result->trans_arena_,
                                           sleep_ms);
  }

  WorldBarrier(&comm);

  api::Context::default_buffer_organizer_retries =
    config->num_buffer_organizer_retries;
  api::Context::default_placement_policy = config->default_placement_policy;
  api::Context::default_rr_split = config->default_rr_split;

  RoundRobinState::devices_.reserve(config->num_devices);
  for (DeviceID id = 0; id < config->num_devices; ++id) {
    if (GetNumBuffersAvailable(&result->context_, id)) {
      RoundRobinState::devices_.push_back(id);
    }
  }

  InitRpcClients(&result->rpc_);

  // NOTE(chogan): Can only initialize the neighborhood Targets once the RPC
  // clients have been initialized.
  InitNeighborhoodTargets(&result->context_, &result->rpc_);

  result->is_initialized = true;

  WorldBarrier(&comm);

  return result;
}

namespace api {

std::shared_ptr<Hermes> InitHermes(const char *config_file, bool is_daemon,
                                   bool is_adapter) {
  u16 endian_test = 0x1;
  char *endian_ptr = (char *)&endian_test;
  if (endian_ptr[0] != 1) {
    LOG(FATAL) << "Big endian machines not supported yet." << std::endl;
  }

  hermes::Config config = {};
  const size_t kConfigMemorySize = KILOBYTES(16);
  hermes::u8 config_memory[kConfigMemorySize];

  if (config_file) {
    hermes::Arena config_arena = {};
    hermes::InitArena(&config_arena, kConfigMemorySize, config_memory);
    hermes::ParseConfig(&config_arena, config_file, &config);
  } else {
    InitDefaultConfig(&config);
  }

  std::shared_ptr<Hermes> result = InitHermes(&config, is_daemon, is_adapter);

  return result;
}

}  // namespace api

std::shared_ptr<api::Hermes> InitHermesClient(const char *config_file) {
  std::shared_ptr<api::Hermes> result =
    api::InitHermes(config_file, false, true);

  return result;
}

std::shared_ptr<api::Hermes> InitHermesDaemon(char *config_file) {
  std::shared_ptr<api::Hermes> result = api::InitHermes(config_file, true);

  return result;
}

std::shared_ptr<api::Hermes> InitHermesDaemon(Config *config) {
  std::shared_ptr<api::Hermes> result = InitHermes(config, true, false);

  return result;
}

}  // namespace hermes
