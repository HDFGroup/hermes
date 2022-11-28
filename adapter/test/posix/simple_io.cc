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
#include <string>
#include <dlfcn.h>
#include <iostream>
#include <glog/logging.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>

int main(int argc, char **argv) {
  int rank;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  if (argc != 6) {
    std::cout << "USAGE: ./posix_simple_io"
              << " [path] [rw] [block_size (kb)] [count] [lag (sec)]";
    exit(1);
  }

  char *path = argv[1];
  int rw = atoi(argv[2]);
  int block_size = atoi(argv[3])*1024;
  int count = atoi(argv[4]);
  int lag = atoi(argv[5]);
  size_t size = count * block_size;
  int off = rank * size;

  std::stringstream ss;
  ss << "RANK: " << rank << std::endl
     << " PATH: " << path << std::endl
     << " Block Size (KB): " << block_size << std::endl
     << " Count: " << count  << std::endl
     << " Total Size (MB): " << size / count << std::endl;
  std::cout << ss.str() << std::endl;

  char *buf = (char*)malloc(size);
  int fd = open(path, O_CREAT | O_RDWR, 0666);
  lseek(fd, off, SEEK_SET);

  for (int i = 0; i < count; ++i) {
    char nonce = i;
    if (rw == 0) {
      memset(buf, nonce, size);
      write(fd, buf, size);
    } else {
      memset(buf, 0, size);
      read(fd, buf, size);
      VerifyBuffer(buf, nonce, size);
    }
  }

  MPI_Finalize();
}