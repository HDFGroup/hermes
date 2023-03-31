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
#include "logging.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include "test/test_utils.h"

int main(int argc, char **argv) {
  int rank = 0, nprocs = 1;
  MPI_Init(&argc, &argv);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  if (argc != 7) {
    std::cout << "USAGE: ./posix_simple_io"
              << " [path] [read] [block_size (kb)] [count]"
              << " [off (blocks)] [lag (sec)]";
    exit(1);
  }

  char *path = argv[1];
  int do_read = atoi(argv[2]);
  int block_size = atoi(argv[3])*1024;
  int count = atoi(argv[4]);
  int block_off = atoi(argv[5]);
  int lag = atoi(argv[6]);
  if (do_read) {
    count -= block_off;
  } else {
    block_off = 0;
  }
  size_t size = count * block_size;
  size_t total_size = size * nprocs;
  int off = (rank * size) + block_off * block_size;

  {
    std::stringstream ss;
    ss << "RANK: " << rank << std::endl
       << " PATH: " << path << std::endl
       << " READ or WRITE: " << (do_read ? "READ" : "WRITE") << std::endl
       << " Block Off: " << block_off << std::endl
       << " Block Size: " << block_size << std::endl
       << " Count: " << count << std::endl
       << " Proc Size (MB): " << size / (1 << 20) << std::endl
       << " Num Ranks: " << nprocs << std::endl;
    std::cout << ss.str() << std::endl;
  }
  MPI_Barrier(MPI_COMM_WORLD);

  sleep(lag);

  char *buf = (char*)malloc(size);
  int fd = open(path, O_CREAT | O_RDWR, 0666);
  lseek(fd, off, SEEK_SET);

  struct stat st;
  fstat(fd, &st);
  if (do_read && (st.st_size - total_size) > 3) {
    if (rank == 0) {
      std::cout << "File sizes aren't equivalent: "
                << " stat: " << st.st_size
                << " real: " << total_size << std::endl;
    }
    exit(1);
  }

  for (int i = 0; i < count; ++i) {
    char nonce = i + 1;
    if (!do_read) {
      memset(buf, nonce, block_size);
      int ret = write(fd, buf, block_size);
      if (ret != block_size) {
        std::cout << "Buffer write failed!" << std::endl;
      }
    } else {
      memset(buf, 0, block_size);
      int ret = read(fd, buf, block_size);
      if (ret != block_size) {
        std::cout << "Buffer read failed!" << std::endl;
      }
      if (!VerifyBuffer(buf, block_size, nonce)) {
        std::cout << "Buffer verification failed!" << std::endl;
        exit(1);
      }
    }
  }

  close(fd);

  MPI_Barrier(MPI_COMM_WORLD);
  {
    std::stringstream ss;
    ss << "SIMPLE I/O COMPLETED! (rank: " << rank << ")" << std::endl;
    std::cout << ss.str();
  }
  MPI_Finalize();
}
