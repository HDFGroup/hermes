//
// Created by lukemartinlogan on 12/19/22.
//

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
struct ShmHeader<DeviceInfo> : public lipc::ShmBaseHeader {
  /** The human-readable name of the device */
  lipc::ShmArchive<lipc::string> dev_name_;
  /** The I/O interface for the device */
  IoInterface io_api_;
  /** The minimum transfer size of each device */
  size_t block_size_;
  /** The unit of each slab, a multiple of the Device's block size */
  lipc::ShmArchive<lipc::vector<size_t>> slab_sizes_;
  /** The directory the device is mounted on */
  lipc::ShmArchive<lipc::string> mount_dir_;
  /** The file to create on the device */
  lipc::ShmArchive<lipc::string> mount_point_;
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
struct DeviceInfo : public SHM_CONTAINER(DeviceInfo) {
  SHM_CONTAINER_TEMPLATE(DeviceInfo, DeviceInfo)

  /** The human-readable name of the device */
  lipc::mptr<lipc::string> dev_name_;
  /** The unit of each slab, a multiple of the Device's block size */
  lipc::mptr<lipc::vector<size_t>> slab_sizes_;
  /** The directory the device is mounted on */
  lipc::mptr<lipc::string> mount_dir_;
  /** The file to create on the device */
  lipc::mptr<lipc::string> mount_point_;

  void shm_init_main(lipc::ShmArchive<DeviceInfo> *ar,
                     lipc::Allocator *alloc) {
    shm_init_header(ar, alloc);
    dev_name_.shm_init(alloc);
    slab_sizes_.shm_init(alloc);
    mount_dir_.shm_init(alloc);
    mount_point_.shm_init(alloc);
    shm_serialize(ar_);
  }

  void shm_destroy(bool destroy_header = true) {
    SHM_DESTROY_DATA_START
    dev_name_.shm_destroy();
    slab_sizes_.shm_destroy();
    mount_dir_.shm_destroy();
    mount_point_.shm_destroy();
    SHM_DESTROY_DATA_END
    SHM_DESTROY_END
  }

  void shm_serialize(lipc::ShmArchive<DeviceInfo> &ar) const {
    shm_serialize_header(ar.header_ptr_);
    dev_name_ >> header_->dev_name_;
    slab_sizes_ >> header_->slab_sizes_;
    mount_dir_ >> header_->mount_dir_;
    mount_point_ >> header_->mount_point_;
  }

  void shm_deserialize(const lipc::ShmArchive<DeviceInfo> &ar) {
    shm_deserialize_header(ar.header_ptr_);
    dev_name_ << header_->dev_name_;
    slab_sizes_ << header_->slab_sizes_;
    mount_dir_ << header_->mount_dir_;
    mount_point_ << header_->mount_point_;
  }

  void WeakMove(DeviceInfo &other) {
    SHM_WEAK_MOVE_START(SHM_WEAK_MOVE_DEFAULT(DeviceInfo))
    (*header_) = (*other.header_);
    (*dev_name_) = lipc::Move(*other.dev_name_);
    (*slab_sizes_) = lipc::Move(*other.slab_sizes_);
    (*mount_dir_) = lipc::Move(*other.mount_dir_);
    (*mount_point_) = lipc::Move(*other.mount_point_);
    SHM_WEAK_MOVE_END()
  }

  void StrongCopy(const DeviceInfo &other) {
    SHM_STRONG_COPY_START(SHM_STRONG_COPY_DEFAULT(DeviceInfo))
    (*header_) = (*other.header_);
    (*dev_name_) = lipc::Copy(*other.dev_name_);
    (*slab_sizes_) = lipc::Copy(*other.slab_sizes_);
    (*mount_dir_) = lipc::Copy(*other.mount_dir_);
    (*mount_point_) = lipc::Copy(*other.mount_point_);
    SHM_STRONG_COPY_END()
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

  /**
   * Paths prefixed with the following directories are not tracked in Hermes
   * Exclusion list used by darshan at
   * darshan/darshan-runtime/lib/darshan-core.c
   */
  std::vector<std::string> path_exclusions;

  /**
   * Paths prefixed with the following directories are tracked by Hermes even if
   * they share a root with a path listed in path_exclusions
   */
  std::vector<std::string> path_inclusions;

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
