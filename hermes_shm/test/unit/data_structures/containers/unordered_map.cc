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
#include "hermes_shm/data_structures/ipc/unordered_map.h"
#include "hermes_shm/data_structures/ipc/string.h"

using hshm::ipc::MemoryBackendType;
using hshm::ipc::MemoryBackend;
using hshm::ipc::allocator_id_t;
using hshm::ipc::AllocatorType;
using hshm::ipc::Allocator;
using hshm::ipc::MemoryManager;
using hshm::ipc::Pointer;
using hshm::ipc::unordered_map;
using hshm::ipc::string;

#define GET_INT_FROM_KEY(VAR) CREATE_GET_INT_FROM_VAR(Key, key_ret, VAR)
#define GET_INT_FROM_VAL(VAR) CREATE_GET_INT_FROM_VAR(Val, val_ret, VAR)

#define CREATE_KV_PAIR(KEY_NAME, KEY, VAL_NAME, VAL)\
  CREATE_SET_VAR_TO_INT_OR_STRING(Key, KEY_NAME, KEY); \
  CREATE_SET_VAR_TO_INT_OR_STRING(Val, VAL_NAME, VAL);

template<typename Key, typename Val>
void UnorderedMapOpTest() {
  Allocator *alloc = alloc_g;
  auto map_p = hipc::make_uptr<unordered_map<Key, Val>>(alloc, 5);
  auto &map = *map_p;

  // Insert 20 entries into the map (no growth trigger)
  PAGE_DIVIDE("Insert entries") {
    for (int i = 0; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      map.emplace(key, val);
    }
  }

  // Iterate over the map
  PAGE_DIVIDE("Forward iterate") {
    std::vector<int> keys, vals;
    for (auto &entry : map) {
      GET_INT_FROM_KEY(entry.GetKey());
      GET_INT_FROM_VAL(entry.GetVal());
      keys.emplace_back(key_ret);
      vals.emplace_back(val_ret);
    }
    REQUIRE(keys.size() == 20);
    REQUIRE(vals.size() == 20);
    std::sort(keys.begin(), keys.end());
    std::sort(vals.begin(), vals.end());
    for (int i = 0; i < 20; ++i) {
      REQUIRE(keys[i] == i);
      REQUIRE(vals[i] == i);
    }
  }

  // Check if the 20 entries are indexable
  PAGE_DIVIDE("Check if entries are indexable") {
    for (int i = 0; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      REQUIRE((map[key]) == val);
    }
  }

  // Check if 20 entries are findable
  PAGE_DIVIDE("Check if entries are findable") {
    for (int i = 0; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      auto iter = map.find(key);
      hipc::pair<Key, Val> &pair = *iter;
      REQUIRE(pair.GetVal() == val);
    }
  }

  // Re-emplace elements (adding 100 to i)
  PAGE_DIVIDE("Re-emplace elements") {
    for (int i = 0; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, i + 100);
      map.emplace(key, val);
      REQUIRE((map[key]) == val);
    }
  }

  // Modify the fourth map entry (move assignment)
  PAGE_DIVIDE("Modify the fourth map entry") {
    CREATE_KV_PAIR(key, 4, val, 25);
    auto iter = map.find(key);
    hipc::pair<Key, Val>& pair = *iter;
    pair.GetVal() = val;
    REQUIRE(pair.GetVal() == val);
  }

  // Verify the modification took place
  PAGE_DIVIDE("Verify the modification took place") {
    CREATE_KV_PAIR(key, 4, val, 25);
    REQUIRE((map[key]) == val);
  }

  // Modify the fourth map entry (copy assignment)
  PAGE_DIVIDE("Copy assignment test") {
    CREATE_KV_PAIR(key, 4, val, 50);
    auto iter = map.find(key);
    hipc::pair<Key, Val>& pair = *iter;
    pair.GetVal() = val;
    REQUIRE(pair.GetVal() == val);
  }

  // Verify the modification took place
  PAGE_DIVIDE("Verify the copy assignment held") {
    CREATE_KV_PAIR(key, 4, val, 50);
    REQUIRE((map[key]) == val);
  }

  // Modify the fourth map entry (copy assignment)
  PAGE_DIVIDE("Modify the fourth map entry (copy assignment)") {
    CREATE_KV_PAIR(key, 4, val, 100);
    auto &x = map[key];
    x = val;
  }

  // Verify the modification took place
  PAGE_DIVIDE("Verify the modification took place") {
    CREATE_KV_PAIR(key, 4, val, 100);
    REQUIRE(map[key] == val);
  }

  // Remove 15 entries from the map
  PAGE_DIVIDE("Remove 15 entries from the map") {
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
  PAGE_DIVIDE("Try emplace on an existing key") {
    for (int i = 15; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, 100);
      REQUIRE(map.try_emplace(key, val) == false);
    }
    for (int i = 15; i < 20; ++i) {
      CREATE_KV_PAIR(key, i, val, 100);
      GET_INT_FROM_VAL(map[key])
      REQUIRE(val_ret == i + 100);
    }
  }

  // Erase the entire map
  PAGE_DIVIDE("Erase the entire map") {
    map.clear();
    REQUIRE(map.size() == 0);
  }

  // Add 100 entries to the map (should force a growth)
  PAGE_DIVIDE("Add 100 entries to the map") {
    for (int i = 0; i < 100; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      map.emplace(key, val);
    }
    for (int i = 0; i < 100; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      auto iter = map.find(key);
      REQUIRE(iter != map.end());
      hipc::pair<Key, Val>& pair = *iter;
      REQUIRE(pair.GetKey() == key);
      REQUIRE(pair.GetVal() == val);
    }
  }

  // Copy assignment operator
  PAGE_DIVIDE("Copy the map") {
    auto cpy = hipc::make_uptr<unordered_map<Key, Val>>(alloc);
    (*cpy) = map;
    for (int i = 0; i < 100; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      auto iter1 = map.find(key);
      auto iter2 = cpy->find(key);
      REQUIRE(!iter1.is_end());
      hipc::pair<Key, Val>& pair1 = *iter1;
      REQUIRE(pair1.GetKey() == key);
      REQUIRE(pair1.GetVal() == val);

      REQUIRE(!iter2.is_end());
      hipc::pair<Key, Val>& pair2 = *iter2;
      REQUIRE(pair2.GetKey() == key);
      REQUIRE(pair2.GetVal() == val);
    }
  }

  // Move assignment operator
  PAGE_DIVIDE("Move the map") {
    auto cpy = hipc::make_uptr<unordered_map<Key, Val>>(alloc);
    (*cpy) = std::move(map);
    for (int i = 0; i < 100; ++i) {
      CREATE_KV_PAIR(key, i, val, i);
      auto iter = cpy->find(key);
      REQUIRE(!iter.is_end());
      hipc::pair<Key, Val>& pair = *iter;
      REQUIRE(pair.GetKey() == key);
      REQUIRE(pair.GetVal() == val);
    }
    map = std::move(*cpy);
  }
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
