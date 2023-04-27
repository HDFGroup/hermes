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
  off_t off;
  size_t size;
  std::string url = argv[1];
  std::stringstream(argv[2]) >> off;
  std::stringstream(argv[3]) >> size;
  PlacementPolicy dpe = PlacementPolicyConv::to_enum(argv[4]);
  auto stager = DataStagerFactory::Get(url);
  if (size == 0) {
    stager->StageIn(url, dpe);
  } else {
    stager->StageIn(url, off, size, dpe);
  }
  HERMES->Finalize();
  MPI_Finalize();
}
