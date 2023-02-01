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

#ifndef HERMES_ADAPTER_POSIX_NATIVE_H_
#define HERMES_ADAPTER_POSIX_NATIVE_H_

#include <memory>

#include "adapter/filesystem/filesystem.h"
#include "adapter/filesystem/fs_metadata_manager.h"
#include "posix_singleton_macros.h"
#include "posix_api.h"
#include "posix_io_client.h"

namespace hermes::adapter::posix {

/** A class to represent POSIX IO file system */
class PosixFs : public hermes::adapter::fs::Filesystem {
 public:
  PosixFs() : hermes::adapter::fs::Filesystem(IoClientType::kPosix) {}
};

/** Simplify access to the stateless PosixFs Singleton */
#define HERMES_POSIX_FS hermes::EasySingleton<hermes::adapter::posix::PosixFS>::GetInstance()
#define HERMES_POSIX_FS_T hermes::adapter::posix::PosixFs*

}  // namespace hermes::adapter::posix

#endif  // HERMES_ADAPTER_POSIX_NATIVE_H_
