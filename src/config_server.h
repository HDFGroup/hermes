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

#ifndef HERMES_SRC_CONFIG_SERVER_H_
#define HERMES_SRC_CONFIG_SERVER_H_

#include "config.h"

namespace hermes::config {

struct DeviceInfo; /** Forward declaration of DeviceInfo */

/**
 * The type of interface the device exposes
 * */
enum class IoInterface {
  kRam,
  kPosix
};

/**
 * DeviceInfo shared-memory representation
 * */
class DeviceInfo : public hipc::ShmContainer {
  SHM_CONTAINER_TEMPLATE(DeviceInfo, DeviceInfo)

  /** The human-readable name of the device */
  hipc::ShmArchive<hipc::string> dev_name_;
  /** The unit of each slab, a multiple of the Device's block size */
  hipc::ShmArchive<hipc::vector<size_t>> slab_sizes_;
  /** The directory the device is mounted on */
  hipc::ShmArchive<hipc::string> mount_dir_;
  /** The file to create on the device */
  hipc::ShmArchive<hipc::string> mount_point_;
  /** The I/O interface for the device */
  IoInterface io_api_;
  /** The minimum transfer size of each device */
  size_t block_size_;
  /** Device capacity (bytes) */
  size_t capacity_;
  /** Bandwidth of a device (MBps) */
  f32 bandwidth_;
  /** Latency of the device (us)*/
  f32 latency_;
  /** Whether or not the device is shared among all nodes */
  bool is_shared_;
  /** BORG's minimum and maximum capacity threshold for device */
  f32 borg_min_thresh_, borg_max_thresh_;

  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM Constructor. Default. */
  explicit DeviceInfo(hipc::Allocator *alloc) {
    shm_init_container(alloc);
    HSHM_MAKE_AR0(dev_name_, alloc);
    HSHM_MAKE_AR0(slab_sizes_, alloc);
    HSHM_MAKE_AR0(mount_dir_, alloc);
    HSHM_MAKE_AR0(mount_point_, alloc);
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor. From DeviceInfo. */
  explicit DeviceInfo(hipc::Allocator *alloc,
                      const DeviceInfo &other) {
    shm_init_container(alloc);
    shm_strong_copy_constructor_main(alloc, other);
  }

  /** Internal copy */
  void strong_copy(const DeviceInfo &other) {
    io_api_ = other.io_api_;
    block_size_ = other.block_size_;
    capacity_ = other.capacity_;
    bandwidth_ = other.bandwidth_;
    latency_ = other.latency_;
    is_shared_ = other.is_shared_;
    borg_min_thresh_ = other.borg_min_thresh_;
    borg_max_thresh_ = other.borg_max_thresh_;
  }

  /** Copy constructor main. */
  void shm_strong_copy_constructor_main(hipc::Allocator *alloc,
                                        const DeviceInfo &other) {
    strong_copy(other);
    HSHM_MAKE_AR(dev_name_, alloc, *other.dev_name_);
    HSHM_MAKE_AR(slab_sizes_, alloc, *other.slab_sizes_);
    HSHM_MAKE_AR(mount_dir_, alloc, *other.mount_dir_);
    HSHM_MAKE_AR(mount_point_, alloc, *other.mount_point_);
  }

  /** SHM copy assignment operator. From DeviceInfo. */
  DeviceInfo& operator=(const DeviceInfo &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_op_main(other);
    }
    return *this;
  }

  /** Copy assignment operator main. */
  void shm_strong_copy_op_main(const DeviceInfo &other) {
    strong_copy(other);
    (*dev_name_) = (*other.dev_name_);
    (*slab_sizes_) = (*other.slab_sizes_);
    (*mount_dir_) = (*other.mount_dir_);
    (*mount_point_) = (*other.mount_point_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  DeviceInfo(hipc::Allocator *alloc,
             DeviceInfo &&other) {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      strong_copy(other);
      HSHM_MAKE_AR(dev_name_, alloc, std::move(*other.dev_name_));
      HSHM_MAKE_AR(slab_sizes_, alloc, std::move(*other.slab_sizes_));
      HSHM_MAKE_AR(mount_dir_, alloc, std::move(*other.mount_dir_));
      HSHM_MAKE_AR(mount_point_, alloc, std::move(*other.mount_point_));
      other.SetNull();
    } else {
      shm_strong_copy_constructor_main(alloc, other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. */
  DeviceInfo& operator=(DeviceInfo &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (GetAllocator() == other.GetAllocator()) {
        strong_copy(other);
        (*dev_name_) = std::move(*other.dev_name_);
        (*slab_sizes_) = std::move(*other.slab_sizes_);
        (*mount_dir_) = std::move(*other.mount_dir_);
        (*mount_point_) = std::move(*other.mount_point_);
        other.SetNull();
      } else {
        shm_strong_copy_op_main(other);
        other.shm_destroy();
      }
    }
    return *this;
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** Whether DeviceInfo is NULL */
  bool IsNull() { return false; }

  /** Set DeviceInfo to NULL */
  void SetNull() {}

  /** Free shared memory */
  void shm_destroy_main() {
    (*dev_name_).shm_destroy();
    (*slab_sizes_).shm_destroy();
    (*mount_dir_).shm_destroy();
    (*mount_point_).shm_destroy();
  }
};

/**
 * RPC information defined in server config
 * */
struct RpcInfo {
  /** The name of a file that contains host names, 1 per line */
  std::string host_file_;
  /** The parsed hostnames from the hermes conf */
  std::vector<std::string> host_names_;
  /** The RPC protocol to be used. */
  std::string protocol_;
  /** The RPC domain name for verbs transport. */
  std::string domain_;
  /** The RPC port number. */
  int port_;
  /** The number of handler threads per RPC server. */
  int num_threads_;
};

/**
 * DPE information defined in server config
 * */
struct DpeInfo {
  /** The default blob placement policy. */
  api::PlacementPolicy default_policy_;

  /** Whether blob splitting is enabled for Round-Robin blob placement. */
  bool default_rr_split_;
};

/**
 * Buffer organizer information defined in server config
 * */
struct BorgInfo {
  /** The number of buffer organizer threads. */
  int num_threads_;
  /** Interval (seconds) where blobs are checked for flushing */
  size_t flush_period_;
  /** Interval (seconds) where blobs are checked for re-organization */
  size_t blob_reorg_period_;
  /** Time when score is equal to 1 (seconds) */
  float recency_min_;
  /** Time when score is equal to 0 (seconds) */
  float recency_max_;
  /** Number of accesses for score to be equal to 1 (count) */
  float freq_max_;
  /** Number of accesses for score to be equal to 0 (count) */
  float freq_min_;
};

/**
 * Prefetcher information in server config
 * */
struct PrefetchInfo {
  bool enabled_;
  std::string trace_path_;
  size_t epoch_ms_;
  bool is_mpi_;
};

/**
 * Tracing information in server config
 * */
struct TracingInfo {
  bool enabled_;
  std::string output_;
};

/**
 * System configuration for Hermes
 */
class ServerConfig : public BaseConfig {
 public:
  /** The device information */
  hipc::uptr<hipc::vector<DeviceInfo>> devices_;

  /** The RPC information */
  RpcInfo rpc_;

  /** The DPE information */
  DpeInfo dpe_;

  /** Buffer organizer (BORG) information */
  BorgInfo borg_;

  /** Tracing information */
  TracingInfo tracing_;

  /** Prefetcher information */
  PrefetchInfo prefetcher_;

  /** Trait repo information */
  std::vector<std::string> trait_paths_;

  /** The length of a view state epoch */
  u32 system_view_state_update_interval_ms;

  /** The max amount of memory hermes uses for non-buffering tasks */
  size_t max_memory_;

  /** A base name for the BufferPool shared memory segement. Hermes appends the
   * value of the USER environment variable to this string.
   */
  std::string shmem_name_;

 public:
  ServerConfig() = default;
  void LoadDefault();

 private:
  void ParseYAML(YAML::Node &yaml_conf);
  void CheckConstraints();
  void ParseRpcInfo(YAML::Node yaml_conf);
  void ParseDeviceInfo(YAML::Node yaml_conf);
  void ParseDpeInfo(YAML::Node yaml_conf);
  void ParseBorgInfo(YAML::Node yaml_conf);
  void ParsePrefetchInfo(YAML::Node yaml_conf);
  void ParseTracingInfo(YAML::Node yaml_conf);
  void ParseTraitInfo(YAML::Node yaml_conf);
};

}  // namespace hermes::config

namespace hermes {
using config::ServerConfig;
}  // namespace hermes

#endif  // HERMES_SRC_CONFIG_SERVER_H_
