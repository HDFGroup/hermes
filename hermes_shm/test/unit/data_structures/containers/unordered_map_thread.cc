/*
 * Copyright (C) 2022  SCS Lab <scslab@iit.edu>,
 * Luke Logan <llogan@hawk.iit.edu>,
 * Jaime Cernuda Garcia <jcernudagarcia@hawk.iit.edu>
 * Jay Lofstead <gflofst@sandia.gov>,
 * Anthony Kougkas <akougkas@iit.edu>,
 * Xian-He Sun <sun@iit.edu>
 *
 * This file is part of HermesShm
 *
 * HermesShm is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/>.
 */

#include "omp.h"
#include "basic_test.h"
#include "test_init.h"
#include "hermes_shm/data_structures/thread_safe/unordered_map.h"
#include "hermes_shm/data_structures/string.h"
#include "hermes_shm/memory/allocator/stack_allocator.h"
#include "hermes_shm/util/errors.h"
#include <sstream>

using hermes_shm::ipc::MemoryBackendType;
using hermes_shm::ipc::MemoryBackend;
using hermes_shm::ipc::allocator_id_t;
using hermes_shm::ipc::AllocatorType;
using hermes_shm::ipc::Allocator;
using hermes_shm::ipc::MemoryManager;
using hermes_shm::ipc::Pointer;
using hermes_shm::ipc::unordered_map;
using hermes_shm::ipc::string;

void UnorderedMapParallelInsert() {
  Allocator *alloc = alloc_g;
  unordered_map<string, string> map(alloc);

  int entries_per_thread = 50;
  int nthreads = 4;
  int total_entries = nthreads * entries_per_thread;
  HERMES_SHM_THREAD_MANAGER->GetThreadStatic();

  omp_set_dynamic(0);
#pragma omp parallel shared(alloc, map) num_threads(nthreads)
  {
    int rank = omp_get_thread_num();
    int off = rank*entries_per_thread;
#pragma omp barrier
    // Insert entries into the map (no growth trigger)
    {
      for (int i = 0; i < entries_per_thread; ++i) {
        int key = off + i;
        int val = 2 * key;
        auto t1 = string(std::to_string(key));
        auto t2 = string(std::to_string(val));

        {
          std::stringstream ss;
          ss << "Emplace start: " << t1.str() << std::endl;
          std::cout << ss.str();
        }
        map.emplace(t1, t2);
        {
          std::stringstream ss;
          ss << "Emplace end: " << t1.str() << std::endl;
          std::cout << ss.str();
        }
      }
    }
#pragma omp barrier
  }
  REQUIRE(map.size() == total_entries);

  // Verify the map has all entries
  for (int i = 0; i < total_entries; ++i) {
    auto key = string(std::to_string(i));
    auto val = string(std::to_string(2*i));
    REQUIRE(*(map[key]) == val);
  }
}

TEST_CASE("UnorderedMapParallelInsert") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  UnorderedMapParallelInsert();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
