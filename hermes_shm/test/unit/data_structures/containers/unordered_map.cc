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

#include "basic_test.h"
#include "test_init.h"
#include "hermes_shm/data_structures/thread_unsafe/unordered_map.h"
#include "hermes_shm/data_structures/string.h"
#include "hermes_shm/memory/allocator/stack_allocator.h"

using hermes_shm::ipc::MemoryBackendType;
using hermes_shm::ipc::MemoryBackend;
using hermes_shm::ipc::allocator_id_t;
using hermes_shm::ipc::AllocatorType;
using hermes_shm::ipc::Allocator;
using hermes_shm::ipc::MemoryManager;
using hermes_shm::ipc::Pointer;
using hermes_shm::ipc::unordered_map;
using hermes_shm::ipc::string;

#define GET_INT_FROM_KEY(VAR) CREATE_GET_INT_FROM_VAR(Key, key_ret, VAR)
#define GET_INT_FROM_VAL(VAR) CREATE_GET_INT_FROM_VAR(Val, val_ret, VAR)

#define CREATE_KV_PAIR(KEY_NAME, KEY, VAL_NAME, VAL)\
  CREATE_SET_VAR_TO_INT_OR_STRING(Key, KEY_NAME, KEY); \
  CREATE_SET_VAR_TO_INT_OR_STRING(Val, VAL_NAME, VAL);

template<typename Key, typename Val>
void UnorderedMapOpTest() {
  Allocator *alloc = alloc_g;
  unordered_map<Key, Val> map(alloc);

  // Insert 20 entries into the map (no growth trigger)
  {
    for (int i = 0; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      map.emplace(key, val);
    }
  }

  // Check if the 20 entries are indexable
  {
    for (int i = 0; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      REQUIRE(*(map[key]) == val);
    }
  }

  // Check if 20 entries are findable
  {
    for (int i = 0; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      auto iter = map.find(key);
      REQUIRE((*iter)->GetVal() == val);
    }
  }

  // Iterate over the map
  {
    // auto prep = map.iter_prep();
    // prep.Lock();
    int i = 0;
    for (auto entry : map) {
      GET_INT_FROM_KEY(entry->GetKey());
      GET_INT_FROM_VAL(entry->GetVal());
      REQUIRE((0 <= key_ret && key_ret < 20));
      REQUIRE((0 <= val_ret && val_ret < 20));
      ++i;
    }
    REQUIRE(i == 20);
  }

  // Re-emplace elements
  {
    for (int i = 0; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, i + 100);
      map.emplace(key, val);
      REQUIRE(*(map[key]) == val);
    }
  }

  // Modify the fourth map entry (move assignment)
  {
    CREATE_KV_PAIR(key, 4, val, 25);
    auto iter = map.find(key);
    (*iter)->GetVal() = std::move(val);
    REQUIRE((*iter)->GetVal() == val);
  }

  // Verify the modification took place
  {
    CREATE_KV_PAIR(key, 4, val, 25);
    REQUIRE(*(map[key]) == val);
  }

  // Modify the fourth map entry (copy assignment)
  {
    CREATE_KV_PAIR(key, 4, val, 50);
    auto iter = map.find(key);
    (*iter)->GetVal() = val;
    REQUIRE((*iter)->GetVal() == val);
  }

  // Verify the modification took place
  {
    CREATE_KV_PAIR(key, 4, val, 50);
    REQUIRE(*(map[key]) == val);
  }

  // Modify the fourth map entry (copy assignment)
  {
    CREATE_KV_PAIR(key, 4, val, 100);
    auto x = map[key];
    (*x) = val;
  }

  // Verify the modification took place
  {
    CREATE_KV_PAIR(key, 4, val, 100);
    REQUIRE(*map[key] == val);
  }

  // Remove 15 entries from the map
  {
    for (int i = 0; i < 15; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      map.erase(key);
    }
    REQUIRE(map.size() == 5);
    for (int i = 0; i < 15; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      REQUIRE(map.find(key) == map.end());
    }
  }

  // Attempt to replace an existing key
  {
    for (int i = 15; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, 100);
      REQUIRE(map.try_emplace(key, val) == false);
    }
    for (int i = 15; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, 100);
      REQUIRE(*map[key] != val);
    }
  }

  // Erase the entire map
  {
    map.clear();
    REQUIRE(map.size() == 0);
  }

  // Add 100 entries to the map (should force a growth)
  {
    for (int i = 0; i < 100; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      map.emplace(key, val);
      REQUIRE(map.find(key) != map.end());
    }
    for (int i = 0; i < 100; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      REQUIRE(map.find(key) != map.end());
    }
  }

  // Copy the unordered_map
  {
    unordered_map<Key, Val> cpy(map);
    for (int i = 0; i < 100; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      REQUIRE(map.find(key) != map.end());
      REQUIRE(cpy.find(key) != cpy.end());
    }
  }

  // Move the unordered_map
  {
    unordered_map<Key, Val> cpy = std::move(map);
    for (int i = 0; i < 100; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      REQUIRE(cpy.find(key) != cpy.end());
    }
    map = std::move(cpy);
  }

  // Emplace a move entry into the map

}

TEST_CASE("UnorderedMapOfIntInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  UnorderedMapOpTest<int, int>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("UnorderedMapOfIntString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  UnorderedMapOpTest<int, string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}


TEST_CASE("UnorderedMapOfStringInt") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  UnorderedMapOpTest<string, int>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}

TEST_CASE("UnorderedMapOfStringString") {
  Allocator *alloc = alloc_g;
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
  UnorderedMapOpTest<string, string>();
  REQUIRE(alloc->GetCurrentlyAllocatedSize() == 0);
}
