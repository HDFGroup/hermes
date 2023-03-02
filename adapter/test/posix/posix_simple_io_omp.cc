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
#include "hermes.h"

static const int num_threads = 4;

static bool VerifyBuffer(char *ptr, size_t size, char nonce) {
  for (size_t i = 0; i < size; ++i) {
    if (ptr[i] != nonce) {
      std::cout << (int)ptr[i] << " != " << (int)nonce <<  std::endl;
      return false;
    }
  }
  return true;
}

void thread(const char *path,
            int do_read,
            int block_size,
            int count,
            int block_off) {
  int rank = omp_get_thread_num();
  size_t size = count * block_size;
  size_t total_size = size * num_threads;
  int off = (rank * size) + block_off * block_size;

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

void start_threads(const char *path,
                   int do_read,
                   int block_size,
                   int count,
                   int block_off) {
  int nthreads = 8;
  HERMES_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel shared(path, do_read, block_size, count, block_off) num_threads(nthreads)
  {  // NOLINT
    thread(path, do_read, block_size, count, block_off);
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

  {
    std::stringstream ss;
    ss << "RANK: " << rank << std::endl
       << " PATH: " << path << std::endl
       << " READ or WRITE: " << (do_read ? "READ" : "WRITE") << std::endl
       << " Block Off: " << block_off << std::endl
       << " Block Size: " << block_size << std::endl
       << " Count: " << count << std::endl
       << " Proc Size (MB): " << size / (1 << 20) << std::endl
       << " Num Ranks: " << num_threads << std::endl;
    std::cout << ss.str() << std::endl;
  }
  start_threads(path, do_read, block_size, count, block_off,
                size, total_size, off);
}
