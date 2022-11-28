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

static bool VerifyBuffer(char *ptr, size_t size, char nonce) {
  for (size_t i = 0; i < size; ++i) {
    if (ptr[i] != nonce) {
      std::cout << (int)ptr[i] << " != " << (int)nonce <<  std::endl;
      return false;
    }
  }
  return true;
}

int main(int argc, char **argv) {
  int rank, nprocs;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
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
  size_t total_size = size * nprocs;
  int off = rank * size;

  std::stringstream ss;
  ss << "RANK: " << rank << std::endl
     << " PATH: " << path << std::endl
     << " READ or WRITE: " << (rw ? "READ" : "WRITE") << std::endl
     << " Block Size: " << block_size << std::endl
     << " Count: " << count  << std::endl
     << " Proc Size (MB): " << size / (1<<20) << std::endl;
  std::cout << ss.str() << std::endl;

  sleep(lag);

  char *buf = (char*)malloc(size);
  int fd = open(path, O_CREAT | O_RDWR, 0666);
  lseek(fd, off, SEEK_SET);

  struct stat st;
  __fxstat(_STAT_VER, fd, &st);
  if (rw && (st.st_size - total_size) > 3) {
    if (rank == 0) {
      std::cout << "File sizes aren't equivalent: "
                << " stat: " << st.st_size
                << " real: " << total_size << std::endl;
    }
    exit(1);
  }

  for (int i = 0; i < count; ++i) {
    char nonce = i;
    if (rw == 0) {
      memset(buf, nonce, block_size);
      write(fd, buf, block_size);
    } else {
      memset(buf, 0, block_size);
      read(fd, buf, block_size);
      if(!VerifyBuffer(buf, block_size, nonce)) {
        std::cout << "Buffer verification failed!" << std::endl;
        exit(1);
      }
    }
  }

  close(fd);
  MPI_Finalize();
}