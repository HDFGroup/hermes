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
#include "hermes_types.h"
#include <hermes.h>
#include "data_stager_factory.h"
#include <stdlib.h>

using hermes::api::PlacementPolicyConv;
using hermes::api::PlacementPolicy;
using hermes::DataStager;
using hermes::DataStagerFactory;

int main(int argc, char **argv) {
  if (argc != 5) {
    std::cout << "Usage: mpirun -n [nprocs]" <<
        " ./stage_in [url] [offset] [size] [dpe]" << std::endl;
    exit(1);
  }
  MPI_Init(&argc, &argv);
  HERMES->Create(hermes::HermesType::kClient);
  std::string url = argv[1];
  off_t off = (off_t) hshm::ConfigParse::ParseSize(argv[2]);
  size_t size = hshm::ConfigParse::ParseSize(argv[3]);
  PlacementPolicy dpe = PlacementPolicyConv::to_enum(argv[4]);
  auto stager = DataStagerFactory::Get(url);
  if (size == 0) {
    HILOG(kInfo, "Full stage-in")
    stager->StageIn(url, dpe);
  } else {
    HILOG(kInfo, "Partial stage-in")
    stager->StageIn(url, off, size, dpe);
  }
  HERMES->Finalize();
  MPI_Finalize();
}
