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
  MPI_Init(&argc, &argv);
  if (argc != 2) {
    std::cout << "Usage: ./stage_out [url]" << std::endl;
    exit(1);
  }
  setenv("HERMES_STOP_DAEMON", "0", false);
  setenv("HERMES_ADAPTER_MODE", "DEFAULT", true);
  setenv("HERMES_CLIENT", "1", true);
  auto mdm = Singleton<hermes::adapter::fs::MetadataManager>::GetInstance();
  mdm->InitializeHermes(true);
  std::string url = argv[1];
  auto stager = DataStagerFactory::Get(url);
  stager->StageOut(url);
  mdm->FinalizeHermes();
  MPI_Finalize();
}
