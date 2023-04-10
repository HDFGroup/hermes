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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_QUEUE_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_QUEUE_H_

namespace hshm {

/** Represents the internal qtok type */
typedef uint64_t _qtok_t;

/** Represents a ticket in the queue */
struct qtok_t {
  _qtok_t id_;

  /** Default constructor */
  qtok_t() = default;

  /** Emplace constructor */
  explicit qtok_t(_qtok_t id) : id_(id) {}

  /** Copy constructor */
  qtok_t(const qtok_t &other) : id_(other.id_) {}

  /** Move constructor */
  qtok_t(qtok_t &&other) : id_(other.id_) {
    other.SetNull();
  }

  /** Set to the null qtok */
  void SetNull() {
    id_ = qtok_t::GetNull().id_;
  }

  /** Get the null qtok */
  static qtok_t GetNull() {
    static qtok_t other(std::numeric_limits<_qtok_t>::max());
    return other;
  }

  /** Check if null */
  bool IsNull() const {
    return id_ == GetNull().id_;
  }
};

}  // namespace hshm

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_QUEUE_H_
