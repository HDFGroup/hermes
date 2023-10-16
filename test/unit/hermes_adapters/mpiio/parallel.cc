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

#include "mpi.h"
#include <vector>
#include <string>
#include <filesystem>
#include <cstdio>

namespace stdfs = std::filesystem;

int main(int argc, char **argv) {
  MPI_File f;
  MPI_Status status;
  int count = 1024 * 1024 / 8;
  int rank, nprocs;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  std::string path = argv[1];
  std::vector<char> buf(count, rank);
  if (rank == 0) {
    FILE *fp = fopen(path.c_str(), "w");
    std::vector<char> init(count*nprocs, -1);
    fwrite(init.data(), 1, count*nprocs, fp);
    fclose(fp);
  }
  MPI_Barrier(MPI_COMM_WORLD);

  MPI_File_open(MPI_COMM_WORLD, path.c_str(), MPI_MODE_WRONLY | MPI_MODE_CREATE,
                MPI_INFO_NULL, &f);
  MPI_File_write_at(f, rank*count, buf.data(), count,
                 MPI_CHAR, &status);
  MPI_File_sync(f);
  MPI_File_close(&f);
  MPI_Finalize();
}
