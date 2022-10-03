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
#include <hermes.h>

using hermes::adapter::posix::PosixFS;
using hermes::api::PlacementPolicyConv;
using hermes::api::PlacementPolicy;
using hermes::adapter::fs::IoOptions;

/* Stage in a single file */
void StageIn(std::string path, int off, int size, PlacementPolicy dpe) {
  auto fs_api = PosixFS();
  void *buf = malloc(size);
  AdapterStat stat;
  bool stat_exists;
  File f = fs_api.Open(stat, path);
  fs_api.Read(f, stat, buf, off, size,
              IoOptions::WithParallelDpe(dpe));
  fs_api.Close(f, stat_exists, false);
  free(buf);
}

int main(int argc, char **argv) {
  auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
  MPI_Init(&argc, &argv);
  mdm->InitializeHermes(true);
  off_t off;
  size_t size;
  std::string path = argv[1];
  std::stringstream(argv[2]) >> off;
  std::stringstream(argv[3]) >> size;
  PlacementPolicy dpe = PlacementPolicyConv::to_enum(argv[4]);

  size_t per_proc_size = size / mdm->comm_size;
  size_t per_proc_off = off + per_proc_size * mdm->rank;
  if (mdm->rank == mdm->comm_size - 1) {
    per_proc_size += size % mdm->comm_size;
  }

  StageIn(path,
          per_proc_off,
          per_proc_size,
          dpe);
  mdm->FinalizeHermes();
  MPI_Finalize();
}
