//
// Created by lukemartinlogan on 9/5/23.
//

#ifndef LABSTOR_INCLUDE_LABSTOR_NETWORK_LOCAL_SERIALIZE_H_
#define LABSTOR_INCLUDE_LABSTOR_NETWORK_LOCAL_SERIALIZE_H_

#include "hermes_shm/data_structures/data_structure.h"

namespace labstor {

/** A class for serializing simple objects into private memory */
template<typename DataT = hshm::charbuf>
class LocalSerialize {
 public:
  DataT &data_;
 public:
  LocalSerialize(DataT &data) : data_(data) {
    data_.resize(0);
  }

  /** left shift operator */
  template<typename T>
  HSHM_ALWAYS_INLINE
  LocalSerialize& operator<<(const T &obj) {
    if constexpr(std::is_arithmetic<T>::value) {
      size_t size = sizeof(T);
      size_t off = data_.size();
      data_.resize(off + size);
      memcpy(data_.data() + off, &obj, size);
    } else if constexpr (std::is_same<T, std::string>::value || std::is_same<T, hshm::charbuf>::value) {
      size_t size = obj.size();
      size_t off = data_.size();
      data_.resize(off + size);
      memcpy(data_.data() + off, obj.data(), size);
    } else {
      throw std::runtime_error("Cannot serialize object");
    }
    return *this;
  }
};

/** A class for serializing simple objects into private memory */
template<typename DataT = hshm::charbuf>
class LocalDeserialize {
 public:
  const DataT &data_;
  size_t cur_off_ = 0;
 public:
  LocalDeserialize(const DataT &data) : data_(data) {
    cur_off_ = 0;
  }

  /** right shift operator */
  template<typename T>
  HSHM_ALWAYS_INLINE
  LocalDeserialize& operator>>(T &obj) {
    size_t size;
    size_t off = cur_off_;
    if constexpr(std::is_arithmetic<T>::value) {
      size = sizeof(T);
      memcpy(&obj, data_.data() + off, size);
    } else if constexpr (std::is_same<T, std::string>::value || std::is_same<T, hshm::charbuf>::value) {
      size = obj.size();
      memcpy(obj.data(), data_.data() + off, size);
    } else {
      throw std::runtime_error("Cannot serialize object");
    }
    cur_off_ += size;
    return *this;
  }
};

}  // namespace labstor

#endif  // LABSTOR_INCLUDE_LABSTOR_NETWORK_LOCAL_SERIALIZE_H_
