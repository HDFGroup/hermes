//
// Created by lukemartinlogan on 8/7/23.
//

#ifndef HRUN_INCLUDE_HRUN_NETWORK_SERIALIZE_H_
#define HRUN_INCLUDE_HRUN_NETWORK_SERIALIZE_H_

#include "hrun/hrun_types.h"
#include "hrun/task_registry/task.h"
#include <sstream>
#include <cereal/archives/binary.hpp>
#include <cereal/types/vector.hpp>
#include <cereal/types/string.hpp>
#include <cereal/types/list.hpp>
#include <cereal/types/unordered_map.hpp>
#include <cereal/types/unordered_set.hpp>
#include <cereal/types/atomic.hpp>

namespace hrun {

/** Receiver will read from data_ */
#define DT_RECEIVER_READ BIT_OPT(u32, 0)

/** Receiver will write to data_ */
#define DT_RECEIVER_WRITE BIT_OPT(u32, 1)

/** Free data_ when the data transfer is complete */
#define DT_FREE_DATA BIT_OPT(u32, 2)

/** Indicate how data should be transferred over network */
template<bool NO_XFER>
struct DataTransferBase {
  hshm::bitfield32_t flags_;  /**< Indicates how data will be accessed */
  void *data_;                /**< The virtual address of data on the node */
  size_t data_size_;          /**< The amount of data to transfer */
  DomainId node_id_;          /**< The node data is located */

  /** Serialize a data transfer object */
  template<typename Ar>
  void serialize(Ar &ar) const {
    ar(flags_, (size_t)data_, data_size_, node_id_);
  }

  /** Default constructor */
  DataTransferBase() = default;

  /** Emplace constructor */
  DataTransferBase(u32 flags, void *data, size_t data_size,
                   const DomainId &node_id = DomainId::GetLocal()) :
  flags_(flags), data_(data), data_size_(data_size), node_id_(node_id) {}

  /** Copy constructor */
  DataTransferBase(const DataTransferBase &xfer) :
  flags_(xfer.flags_), data_(xfer.data_),
  data_size_(xfer.data_size_), node_id_(xfer.node_id_) {}

  /** Copy assignment */
  DataTransferBase& operator=(const DataTransferBase &xfer) {
    flags_ = xfer.flags_;
    data_ = xfer.data_;
    data_size_ = xfer.data_size_;
    node_id_ = xfer.node_id_;
    return *this;
  }

  /** Move constructor */
  DataTransferBase(DataTransferBase &&xfer) noexcept :
  flags_(xfer.flags_), data_(xfer.data_),
  data_size_(xfer.data_size_), node_id_(xfer.node_id_) {}

  /** Equality operator */
  bool operator==(const DataTransferBase &other) const {
    return flags_.bits_ == other.flags_.bits_ &&
         data_ == other.data_ &&
         data_size_ == other.data_size_ &&
         node_id_ == other.node_id_;
  }
};

/** A sized data pointer indicating data I/O direction */
struct DataPointer {
  hshm::bitfield32_t flags_;  /**< Indicates how data will be accessed */
  hipc::Pointer data_;        /**< The SHM address of data on the node */
  size_t data_size_;          /**< The amount of data to transfer */
};

using DataTransfer = DataTransferBase<true>;
using PassDataTransfer = DataTransferBase<false>;

/** Serialize a data structure */
template<bool is_start>
class BinaryOutputArchive {
 public:
  std::vector<DataTransfer> xfer_;
  std::stringstream ss_;
  cereal::BinaryOutputArchive ar_;
  DomainId node_id_;

 public:
  /** Default constructor */
  BinaryOutputArchive(const DomainId &node_id)
  : node_id_(node_id), ar_(ss_) {}

  /** Serialize using call */
  template<typename T, typename ...Args>
  BinaryOutputArchive& operator()(T &var, Args &&...args) {
    return Serialize(0, var, std::forward<Args>(args)...);
  }

  /** Serialize using left shift */
  template<typename T>
  BinaryOutputArchive& operator<<(T &var) {
    return Serialize(0, var);
  }

  /** Serialize using ampersand */
  template<typename T>
  BinaryOutputArchive& operator&(T &var) {
    return Serialize(0, var);
  }

  /** Serialize using left shift */
  template<typename T>
  BinaryOutputArchive& operator<<(T &&var) {
    return Serialize(0, var);
  }

  /** Serialize using ampersand */
  template<typename T>
  BinaryOutputArchive& operator&(T &&var) {
    return Serialize(0, var);
  }

  /** Serialize an array */
  template<typename T>
  BinaryOutputArchive& write(T *data, size_t count) {
    size_t size = count * sizeof(T);
    return Serialize(0, cereal::binary_data(data, size));
  }

  /** Serialize a parameter */
  template<typename T, typename ...Args>
  BinaryOutputArchive& Serialize(u32 replica, T &var, Args&& ...args) {
    if constexpr (IS_TASK(T)) {
      if constexpr (IS_SRL(T)) {
        if constexpr (is_start) {
          if constexpr (USES_SRL_START(T)) {
            var.SerializeStart(*this);
          } else {
            var.SaveStart(*this);
          }
        } else {
          if constexpr (USES_SRL_END(T)) {
            var.SerializeEnd(replica, *this);
          } else {
            var.SaveEnd(*this);
          }
        }
      }
    } else if constexpr (std::is_same_v<T, DataTransfer>){
      var.node_id_ = node_id_;
      xfer_.emplace_back(var);
    } else if constexpr(std::is_same_v<T, DataPointer>) {
      DataTransfer xfer;
      xfer.flags_ = var.flags_;
      xfer.data_ = HERMES_MEMORY_MANAGER->Convert<char>(var.data_);
      xfer.data_size_ = var.size_;
      xfer_.emplace_back(var);
    } else {
      ar_ << var;
    }
    return Serialize(replica, std::forward<Args>(args)...);
  }

  /** End serialization recursion */
  BinaryOutputArchive& Serialize(u32 replica) {
    (void) replica;
    return *this;
  }

  /** Get serialized data */
  std::vector<DataTransfer> Get() {
    // Serialize metadata parameters
    std::string str = ss_.str();
    void *data = nullptr;
    if (str.size() > 0) {
      data = malloc(str.size());
      memcpy(data, str.data(), str.size());
      ss_.clear();
    }
    xfer_.emplace_back(DT_RECEIVER_READ | DT_FREE_DATA, data, str.size(), node_id_);

    // Return transfer buffers
    return std::move(xfer_);
  }
};

/** Desrialize a data structure */
template<bool is_start>
class BinaryInputArchive {
 public:
  std::vector<DataTransfer> xfer_;
  std::stringstream ss_;
  cereal::BinaryInputArchive ar_;
  int xfer_off_;

 public:
  /** Default constructor */
  BinaryInputArchive(std::vector<DataTransfer> &xfer)
  : xfer_(std::move(xfer)), xfer_off_(0), ss_(), ar_(ss_) {
    auto &param_xfer = xfer_.back();
    ss_.str(std::string((char*)param_xfer.data_, param_xfer.data_size_));
  }

  /** Deserialize using call */
  template<typename T, typename ...Args>
  BinaryInputArchive& operator()(T &var, Args &&...args) {
    return Deserialize(0, var, std::forward<Args>(args)...);
  }

  /** Deserialize using right shift */
  template<typename T>
  BinaryInputArchive& operator>>(T &var) {
    return Deserialize(0, var);
  }

  /** Deserialize using ampersand */
  template<typename T>
  BinaryInputArchive& operator&(T &var) {
    return Deserialize(0, var);
  }

  /** Deserialize an array */
  template<typename T>
  BinaryInputArchive& read(T *data, size_t count) {
    size_t size = count * sizeof(T);
    Deserialize(0, cereal::binary_data(data, size));
  }

  /** Deserialize a parameter */
  template<typename T, typename ...Args>
  BinaryInputArchive& Deserialize(u32 replica, T &var, Args&& ...args) {
    if constexpr (IS_TASK(T)) {
      if constexpr (IS_SRL(T)) {
        if constexpr (is_start) {
          if constexpr (USES_SRL_START(T)) {
            var.SerializeStart(*this);
          } else {
            var.LoadStart(*this);
          }
        } else {
          if constexpr (USES_SRL_END(T)) {
            var.SerializeEnd(replica, *this);
          } else {
            var.LoadEnd(replica, *this);
          }
        }
      }
    } else if constexpr (std::is_same_v<T, DataTransfer>) {
      var = xfer_[xfer_off_++];
    } else if constexpr(std::is_same_v<T, DataPointer>) {
      DataTransfer &xfer = xfer_[xfer_off_++];
      var.shm_ = HERMES_MEMORY_MANAGER->Convert<char>(xfer.data_);
      var.size_ = xfer.data_size_;
      var.flags_ = xfer.flags_;
    } else {
      ar_ >> var;
    }
    return Deserialize(replica, std::forward<Args>(args)...);
  }

  /** End deserialize recursion */
  HSHM_ALWAYS_INLINE
  BinaryInputArchive& Deserialize(u32 replica) {
    return *this;
  }
};

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_NETWORK_SERIALIZE_H_
