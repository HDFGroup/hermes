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

#ifndef HERMES_SYSINFO_INFO_H_
#define HERMES_SYSINFO_INFO_H_

#include <unistd.h>
#include <sys/sysinfo.h>

namespace hermes_shm {

struct SystemInfo {
  int pid_;
  int ncpu_;
  int page_size_;
  size_t ram_size_;

  SystemInfo() {
    pid_ = getpid();
    ncpu_ = get_nprocs_conf();
    page_size_ = getpagesize();
    struct sysinfo info;
    sysinfo(&info);
    ram_size_ = info.totalram;
  }
};

}  // namespace hermes_shm

#endif  // HERMES_SYSINFO_INFO_H_
