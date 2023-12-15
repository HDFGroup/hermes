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

#ifndef HRUN_INCLUDE_HRUN_NETWORK_LOCAL_SERIALIZE_H_
#define HRUN_INCLUDE_HRUN_NETWORK_LOCAL_SERIALIZE_H_

#include "hermes_shm/data_structures/data_structure.h"

namespace hrun {

/** A class for serializing simple objects into private memory */
template<typename DataT = hshm::charbuf>
class LocalSerialize {
 public:
  DataT &data_;
 public:
  LocalSerialize(DataT &data) : data_(data) {
    data_.resize(0);
  }
  LocalSerialize(DataT &data, bool) : data_(data) {}

  /** left shift operator */
  template<int T>
  HSHM_ALWAYS_INLINE
  LocalSerialize& operator<<(const UniqueId<T> &obj) {
    (*this) << obj.unique_;
    (*this) << obj.node_id_;
    return *this;
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
    } else if constexpr (std::is_same<T, std::string>::value ||
                         std::is_same<T, hshm::charbuf>::value) {
      size_t size = sizeof(size_t) + obj.size();
      size_t off = data_.size();
      data_.resize(off + size);
      memcpy(data_.data() + off, &size, sizeof(size_t));
      off += sizeof(size_t);
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
  template<int T>
  HSHM_ALWAYS_INLINE
  LocalDeserialize& operator<<(const UniqueId<T> &obj) {
    (*this) >> obj.unique_;
    (*this) >> obj.node_id_;
    return *this;
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
      memcpy(&size, data_.data() + off, sizeof(size_t));
      size_t str_size = size - sizeof(size_t);
      off += sizeof(size_t);
      obj.resize(str_size);
      memcpy(obj.data(), data_.data() + off, str_size);
    } else {
      throw std::runtime_error("Cannot serialize object");
    }
    cur_off_ += size;
    return *this;
  }
};

}  // namespace hrun

#endif  // HRUN_INCLUDE_HRUN_NETWORK_LOCAL_SERIALIZE_H_
