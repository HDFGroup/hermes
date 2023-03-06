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

#include "omp.h"
#include "hermes.h"
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
#include "test/test_utils.h"

static const int kNumProcs = 4;

void TestThread(char *path,
                int do_read,
                int block_size,
                int count,
                int block_off) {
  int rank = omp_get_thread_num();
  size_t size = count * block_size;
  size_t total_size = size * kNumProcs;
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
       << " Num Ranks: " << kNumProcs << std::endl;
    std::cout << ss.str() << std::endl;
  }
#pragma omp barrier

>>>>>>> new-borg
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
      write(fd, buf, block_size);
    } else {
      memset(buf, 0, block_size);
      read(fd, buf, block_size);
      if (!VerifyBuffer(buf, block_size, nonce)) {
        std::cout << "Buffer verification failed!" << std::endl;
        exit(1);
      }
    }
  }

  close(fd);

#pragma omp barrier
  {
    std::stringstream ss;
    ss << "SIMPLE I/O COMPLETED! (rank: " << rank << ")" << std::endl;
    std::cout << ss.str();
  }
}

int main(int argc, char **argv) {
  if (argc != 6) {
    std::cout << "USAGE: ./posix_simple_io"
              << " [path] [read] [block_size (kb)] [count]"
              << " [off (blocks)]";
    exit(1);
  }

  char *path = argv[1];
  int do_read = atoi(argv[2]);
  int block_size = atoi(argv[3])*1024;
  int count = atoi(argv[4]);
  int block_off = atoi(argv[5]);
  if (do_read) {
    count -= block_off;
  } else {
    block_off = 0;
  }

  HERMES_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel \
  shared(path, do_read, block_size, count, block_off) num_threads(kNumProcs)
  {  // NOLINT
#pragma omp barrier
    TestThread(path, do_read, block_size, count, block_off);
  }
}
