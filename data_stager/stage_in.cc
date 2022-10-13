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
#include "data_stager_factory.h"

using hermes::api::PlacementPolicyConv;
using hermes::api::PlacementPolicy;
using hermes::DataStager;
using hermes::DataStagerFactory;

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cout << "Usage: mpirun -n [nprocs]" <<
        " ./stage_in [url] [offset] [size] [dpe]" << std::endl;
    exit(1);
  }
  auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
  MPI_Init(&argc, &argv);
  mdm->InitializeHermes(true);
  off_t off;
  size_t size;
  std::string url = argv[1];
  std::stringstream(argv[2]) >> off;
  std::stringstream(argv[3]) >> size;
  PlacementPolicy dpe = PlacementPolicyConv::to_enum(argv[4]);
  auto stager = DataStagerFactory::Get(url);
  stager->StageIn(url, dpe);
  mdm->FinalizeHermes();
  MPI_Finalize();
}
