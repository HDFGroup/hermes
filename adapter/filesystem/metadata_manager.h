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

#ifndef HERMES_ADAPTER_METADATA_MANAGER_H
#define HERMES_ADAPTER_METADATA_MANAGER_H

#include <ftw.h>
#include <mpi.h>

#include <cstdio>
#include <unordered_map>

#include "constants.h"
#include "filesystem.h"

namespace hermes::adapter::fs {
/**
 * Metadata manager for POSIX adapter
 */
class MetadataManager {
 private:
  std::unordered_map<File, AdapterStat> metadata; /**< Map for metadata*/
  int hermes_fd_min_, hermes_fd_max_;  /**< Min and max fd values (inclusive)*/
  std::atomic<int> hermes_fd_cur_; /**< Current fd */
  bool is_init_; /**< Whether hermes is initialized yet */
  std::mutex lock_; /**< Lock for init and metadata */

 public:
  /** map for Hermes request */
  std::unordered_map<uint64_t, HermesRequest*> request_map;

  /** Constructor */
  MetadataManager()
  : metadata(), is_init_(false) {}

  /** Initialize Hermes (thread-safe) */
  void InitializeHermes() {
    if (!is_init_) {
      lock_.lock();
      if (!is_init_) {
        HERMES->Init(HermesType::kClient);
        is_init_ = true;
      }
      lock_.unlock();
    }

    // TODO(llogan): Recycle old fds
    hermes_fd_min_ = 8192;  // TODO(llogan): don't assume 8192
    hermes_fd_max_ = INT_MAX;
  }

  /**
   * Create a metadata entry for POSIX adapter for a given file handler.
   * @param f original file handler of the file on the destination
   * filesystem.
   * @param stat POSIX Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Create(const File& f, const AdapterStat& stat);

  /**
   * Update existing metadata entry for POSIX adapter for a given file handler.
   * @param f original file handler of the file on the destination.
   * @param stat POSIX Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful or entry doesn't exist.
   */
  bool Update(const File& f, const AdapterStat& stat);

  /**
   * Delete existing metadata entry for POSIX adapter for a given file handler.
   * @param f original file handler of the file on the destination.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Delete(const File& f);

  /**
   * Find existing metadata entry for POSIX adapter for a given file handler.
   * @param f original file handler of the file on the destination.
   * @return    The metadata entry if exist.
   *            The bool in pair indicated whether metadata entry exists.
   */
  std::pair<AdapterStat, bool> Find(const File& f);
};
}  // namespace hermes::adapter::fs

#endif  // HERMES_ADAPTER_METADATA_MANAGER_H
