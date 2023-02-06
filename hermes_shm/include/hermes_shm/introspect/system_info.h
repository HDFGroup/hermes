/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

#ifndef HERMES_SHM_SYSINFO_INFO_H_
#define HERMES_SHM_SYSINFO_INFO_H_

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

#endif  // HERMES_SHM_SYSINFO_INFO_H_
