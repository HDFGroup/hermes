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

class DeviceInfo; /** Forward declaration of DeviceInfo */

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
template<>
struct ShmHeader<DeviceInfo> : public hipc::ShmBaseHeader {
  /** The human-readable name of the device */
  hipc::TypedPointer<hipc::string> dev_name_;
  /** The I/O interface for the device */
  IoInterface io_api_;
  /** The minimum transfer size of each device */
  size_t block_size_;
  /** The unit of each slab, a multiple of the Device's block size */
  hipc::TypedPointer<hipc::vector<size_t>> slab_sizes_;
  /** The directory the device is mounted on */
  hipc::TypedPointer<hipc::string> mount_dir_;
  /** The file to create on the device */
  hipc::TypedPointer<hipc::string> mount_point_;
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
};

/**
 * Device information defined in server config
 * */
struct DeviceInfo : public hipc::ShmContainer {
  SHM_CONTAINER_TEMPLATE(DeviceInfo, DeviceInfo, ShmHeader<DeviceInfo>)

  /** The human-readable name of the device */
  hipc::mptr<hipc::string> dev_name_;
  /** The unit of each slab, a multiple of the Device's block size */
  hipc::mptr<hipc::vector<size_t>> slab_sizes_;
  /** The directory the device is mounted on */
  hipc::mptr<hipc::string> mount_dir_;
  /** The file to create on the device */
  hipc::mptr<hipc::string> mount_point_;

  /** Default Constructor */
  DeviceInfo() = default;

  /** Default SHM Constructor */
  void shm_init_main(ShmHeader<DeviceInfo> *header,
                     hipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    dev_name_.shm_init(alloc_);
    slab_sizes_.shm_init(alloc_);
    mount_dir_.shm_init(alloc_);
    mount_point_.shm_init(alloc_);
    shm_serialize_main();
  }

  /** Free shared memory */
  void shm_destroy_main() {
    dev_name_.shm_destroy();
    slab_sizes_.shm_destroy();
    mount_dir_.shm_destroy();
    mount_point_.shm_destroy();
  }

  /** Serialize into SHM */
  void shm_serialize_main() const {
    dev_name_ >> header_->dev_name_;
    slab_sizes_ >> header_->slab_sizes_;
    mount_dir_ >> header_->mount_dir_;
    mount_point_ >> header_->mount_point_;
  }

  /** Deserialize from SHM */
  void shm_deserialize_main() {
    dev_name_ << header_->dev_name_;
    slab_sizes_ << header_->slab_sizes_;
    mount_dir_ << header_->mount_dir_;
    mount_point_ << header_->mount_point_;
  }

  /** Move another object into this object. */
  void shm_weak_move_main(ShmHeader<DeviceInfo> *header,
                          hipc::Allocator *alloc,
                          DeviceInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    (*header_) = (*other.header_);
    (*dev_name_) = std::move(*other.dev_name_);
    (*slab_sizes_) = std::move(*other.slab_sizes_);
    (*mount_dir_) = std::move(*other.mount_dir_);
    (*mount_point_) = std::move(*other.mount_point_);
    shm_serialize_main();
  }

  /** Copy another object into this object */
  void shm_strong_copy_main(ShmHeader<DeviceInfo> *header,
                            hipc::Allocator *alloc,
                            const DeviceInfo &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    (*header_) = (*other.header_);
    (*dev_name_) = (*other.dev_name_);
    (*slab_sizes_) = (*other.slab_sizes_);
    (*mount_dir_) = (*other.mount_dir_);
    (*mount_point_) = (*other.mount_point_);
    shm_serialize_main();
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
  /** The RPC port number for the buffer organizer. */
  int port_;
  /** The number of buffer organizer threads. */
  int num_threads_;
};

/**
 * System configuration for Hermes
 */
class ServerConfig : public BaseConfig {
 public:
  /** The device information */
  std::vector<DeviceInfo> devices_;

  /** The RPC information */
  RpcInfo rpc_;

  /** The DPE information */
  DpeInfo dpe_;

  /** Buffer organizer (BORG) information */
  BorgInfo borg_;

  /** The length of a view state epoch */
  u32 system_view_state_update_interval_ms;

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
};

}  // namespace hermes::config

namespace hermes {
using config::ServerConfig;
}  // namespace hermes

#endif  // HERMES_SRC_CONFIG_SERVER_H_
