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

//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_ABSTRACT_ADAPTER_H
#define HERMES_ABSTRACT_ADAPTER_H

#include "utils.h"
#include "hermes_types.h"

namespace hermes::adapter {

/** Adapter types */
enum class AdapterType {
  kPosix,
  kStdio,
  kMpiio,
  kPubsub,
  kVfd
};

/** A class to represent abstract adapter */
class AbstractAdapter {
 public:
  /**< */
  virtual void PutFallback(const Blob &blob,
                           size_t blob_off,
                           size_t backend_off,
                           size_t backend_size,
                           IoContext &io_ctx) = 0;

  virtual void GetFallback(Blob &blob,
                           size_t blob_off,
                           size_t backend_off,
                           size_t backend_size,
                           IoContext &io_ctx) = 0;

  virtual void LoadBlobFromBackend(size_t backend_off,
                                   size_t backend_size,
                                   IoContext &io_ctx) = 0;
};

}  // namespace hermes::adapter

#endif  // HERMES_ABSTRACT_ADAPTER_H
