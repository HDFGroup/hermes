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

#ifndef HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_TYPES_H_
#define HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_TYPES_H_

#include "adapter/io_client/io_client.h"

namespace hermes::adapter::fs {

/** A structure to represent file */
struct File : public IoClientContext {
  /** default constructor */
  File() = default;

  /** file constructor that copies \a old file */
  File(const File &old) { Copy(old); }

  /** file assignment operator that copies \a old file */
  File &operator=(const File &old) {
    Copy(old);
    return *this;
  }

  /** copy \a old file */
  void Copy(const File &old) {
    filename_ = old.filename_;
    hermes_fd_ = old.hermes_fd_;
    hermes_fh_ = old.hermes_fh_;
    hermes_mpi_fh_ = old.hermes_mpi_fh_;
    status_ = old.status_;
  }

  /** file comparison operator */
  bool operator==(const File &other) const {
    return (hermes_fd_ == other.hermes_fd_) &&
           (hermes_fh_ == other.hermes_fh_) &&
           (hermes_mpi_fh_ == other.hermes_mpi_fh_);
  }

  /** return hash value of this class  */
  std::size_t hash() const {
    std::size_t result;
    std::size_t h1 = std::hash<int>{}(hermes_fd_);
    std::size_t h2 = std::hash<void*>{}(hermes_fh_);
    std::size_t h3 = std::hash<void*>{}(hermes_mpi_fh_);
    result = h1 ^ h2 ^ h3;
    return result;
  }
};

}  // namespace hermes::adapter::fs

namespace std {
/** A structure to represent hash */
template <>
struct hash<hermes::adapter::fs::File> {
  /** hash creator functor */
  std::size_t operator()(const hermes::adapter::fs::File &key) const {
    return key.hash();
  }
};
}  // namespace std

#endif  // HERMES_ADAPTER_FILESYSTEM_FILESYSTEM_TYPES_H_
