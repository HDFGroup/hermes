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

#include <string>
#include "posix/fs_api.h"
#include "hermes_types.h"

using hermes::adapter::posix::PosixFS;
using hermes::api::PlacementPolicyConv;
using hermes::api::PlacementPolicy;

/* Stage in a single file */
void StageIn(std::string path, int off, int size, PlacementPolicy dpe) {
  auto fs_api = PosixFS();
  void *buf = malloc(size);
  AdapterStat stat;
  File f = fs_api.Open(stat, path);
  fs_api.Read(f, stat, buf, off, size, false, dpe);
}

int main(int argc, char **argv) {
  std::string path = argv[1];
  int off = atoi(argv[2]);
  int size = atoi(argv[3]);
  PlacementPolicy dpe = PlacementPolicyConv::to_enum(argv[3]);
  StageIn(path, off, size, dpe);
}