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

#include "filesystem.h"
#include "posix/fs_api.h"

using hermes::adapter::posix::PosixFS;
using hermes::api::PlacementPolicy;
typedef PosixFS MpiioFS;

bool StageIn(std::string path, int off, int size, PlacementPolicy dpe) {
  auto fs_api = MpiioFS();
  void *buf = malloc(size);
  AdapterStat stat;
  File f = fs_api.Open(stat, path);
  fs_api.Read(f, stat, buf, off, size, false, dpe);
  fs_api.FreeFile(f);
}
