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
struct ShmHeader<DeviceInfo> {
  SHM_CONTAINER_HEADER_TEMPLATE(ShmHeader)
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

  /** Internal copy */
  void strong_copy(const ShmHeader &other) {
    io_api_ = other.io_api_;
    block_size_ = other.block_size_;
    capacity_ = other.capacity_;
    bandwidth_ = other.bandwidth_;
    latency_ = other.latency_;
    is_shared_ = other.is_shared_;
    borg_min_thresh_ = other.borg_min_thresh_;
    borg_max_thresh_ = other.borg_max_thresh_;
  }
};

/**
 * Device information defined in server config
 * */
struct DeviceInfo : public hipc::ShmContainer {
  SHM_CONTAINER_TEMPLATE(DeviceInfo, DeviceInfo, ShmHeader<DeviceInfo>)

  /** The human-readable name of the device */
  hipc::Ref<hipc::string> dev_name_;
  /** The unit of each slab, a multiple of the Device's block size */
  hipc::Ref<hipc::vector<size_t>> slab_sizes_;
  /** The directory the device is mounted on */
  hipc::Ref<hipc::string> mount_dir_;
  /** The file to create on the device */
  hipc::Ref<hipc::string> mount_point_;

  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM Constructor. Default. */
  explicit DeviceInfo(ShmHeader<DeviceInfo> *header, hipc::Allocator *alloc) {
    shm_init_header(header, alloc);
    dev_name_ = hipc::make_ref<hipc::string>(
        header_->dev_name_, alloc_);
    slab_sizes_ = hipc::make_ref<hipc::vector<size_t>>(
        header_->slab_sizes_, alloc_);
    mount_dir_ = hipc::make_ref<hipc::string>(
        header_->mount_dir_, alloc_);
    mount_point_ = hipc::make_ref<hipc::string>
        (header_->mount_point_, alloc_);
  }

  /**====================================
   * SHM Deserialization
   * ===================================*/

  /** Deserialize from SHM */
  void shm_deserialize_main() {
    dev_name_ = hipc::Ref<hipc::string>(
        header_->dev_name_, alloc_);
    slab_sizes_ = hipc::Ref<hipc::vector<size_t>>(
        header_->slab_sizes_, alloc_);
    mount_dir_ = hipc::Ref<hipc::string>(
        header_->mount_dir_, alloc_);
    mount_point_ = hipc::Ref<hipc::string>
        (header_->mount_point_, alloc_);
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor. From DeviceInfo. */
  explicit DeviceInfo(ShmHeader<DeviceInfo> *header,
                      hipc::Allocator *alloc,
                      const DeviceInfo &other) {
    shm_init_header(header, alloc);
    shm_strong_copy_constructor_main(other);
  }

  /** Copy constructor main. */
  void shm_strong_copy_constructor_main(const DeviceInfo &other) {
    (*header_) = (*other.header_);
    dev_name_ = hipc::make_ref<hipc::string>(
        header_->dev_name_, alloc_, *other.dev_name_);
    slab_sizes_ = hipc::make_ref<hipc::vector<size_t>>(
        header_->slab_sizes_, alloc_, *other.slab_sizes_);
    mount_dir_ = hipc::make_ref<hipc::string>(
        header_->mount_dir_, alloc_, *other.mount_dir_);
    mount_point_ = hipc::make_ref<hipc::string>
        (header_->mount_point_, alloc_, *other.mount_point_);
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
    (*header_) = (*other.header_);
    (*dev_name_) = (*other.dev_name_);
    (*slab_sizes_) = (*other.slab_sizes_);
    (*mount_dir_) = (*other.mount_dir_);
    (*mount_point_) = (*other.mount_point_);
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  DeviceInfo(ShmHeader<DeviceInfo> *header,
             hipc::Allocator *alloc,
             DeviceInfo &&other) {
    shm_init_header(header, alloc);
    if (alloc_ == other.alloc_) {
      (*header_) = (*other.header_);
      dev_name_ = hipc::make_ref<hipc::string>(
          header_->dev_name_, alloc_, std::move(*other.dev_name_));
      slab_sizes_ = hipc::make_ref<hipc::vector<size_t>>(
          header_->slab_sizes_, alloc_, std::move(*other.slab_sizes_));
      mount_dir_ = hipc::make_ref<hipc::string>(
          header_->mount_dir_, alloc_, std::move(*other.mount_dir_));
      mount_point_ = hipc::make_ref<hipc::string>
          (header_->mount_point_, alloc_, std::move(*other.mount_point_));
      other.SetNull();
    } else {
      shm_strong_copy_constructor_main(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. */
  DeviceInfo& operator=(DeviceInfo &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (alloc_ == other.alloc_) {
        (*header_) = (*other.header_);
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
     dev_name_->shm_destroy();
     slab_sizes_->shm_destroy();
     mount_dir_->shm_destroy();
     mount_point_->shm_destroy();
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
  hipc::uptr<hipc::vector<DeviceInfo>> devices_;

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
