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

// Boost interprocess
#include <boost/unordered_map.hpp>

// Std
#include <string>
#include <unordered_map>

// hermes
#include "hermes_shm/data_structures/ipc/string.h"
#include <hermes_shm/data_structures/ipc/unordered_map.h>

template<typename Key, typename T>
using bipc_unordered_map = boost::unordered_map<
    Key, T,
    std::hash<size_t>, std::equal_to<size_t>,
    typename BoostAllocator<T>::alloc_t>;

/**
 * A series of performance tests for unordered_maps
 * OUTPUT:
 * [test_name] [map_type] [internal_type] [time_ms]
 * */
template<typename T, typename MapT,
  typename MapTPtr=SHM_X_OR_Y(MapT, hipc::mptr<MapT>, MapT*)>
class UnorderedMapTest {
 public:
  std::string map_type_;
  std::string internal_type_;
  MapT *map_;
  MapTPtr map_ptr_;
  void *ptr_;

  /**====================================
   * Test Runner
   * ===================================*/

  /** Test case constructor */
  UnorderedMapTest() {
    if constexpr(std::is_same_v<std::unordered_map<size_t, T>, MapT>) {
      map_type_ = "std::unordered_map";
    } else if constexpr(std::is_same_v<hipc::unordered_map<size_t, T>, MapT>) {
      map_type_ = "hipc::unordered_map";
    } else if constexpr(std::is_same_v<bipc_unordered_map<size_t, T>, MapT>) {
      map_type_ = "bipc::unordered_map";
    } else {
      std::cout << "INVALID: none of the unordered_map tests matched"
      << std::endl;
      return;
    }
    internal_type_ = InternalTypeName<T>::Get();
  }

  /** Run the tests */
  void Test() {
    size_t count = 100000;
    // AllocateTest(count);
    EmplaceTest(count);
    GetTest(count);
    ForwardIteratorTest(count);
    // CopyTest(count);
    // MoveTest(count);
  }

  /**====================================
   * Tests
   * ===================================*/

  /** Test performance of unordered_map allocation */
  void AllocateTest(size_t count) {
    Timer t;
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      Allocate();
      Destroy();
    }
    t.Pause();

    TestOutput("Allocate", t);
  }

  /** Emplace performance */
  void EmplaceTest(size_t count) {
    Timer t;
    Allocate();

    t.Resume();
    Emplace(count);
    t.Pause();

    TestOutput("Emplace", t);
    Destroy();
  }

  /** Get performance */
  void GetTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    Emplace(count);

    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      Get(i);
    }
    t.Pause();

    TestOutput("FixedGet", t);
    Destroy();
  }

  /** Iterator performance */
  void ForwardIteratorTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    Emplace(count);

    t.Resume();
    for (auto &x : *map_) {
      USE(x);
    }
    t.Pause();

    TestOutput("ForwardIterator", t);
    Destroy();
  }

  /** Copy performance */
  void CopyTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    Emplace(count);

    t.Resume();
    if constexpr(IS_SHM_ARCHIVEABLE(MapT)) {
      auto vec2 = hipc::make_uptr<MapT>(*map_);
      USE(vec2)
    } else {
      MapT vec2(*map_);
      USE(vec2)
    }
    t.Pause();

    TestOutput("Copy", t);
    Destroy();
  }

  /** Move performance */
  void MoveTest(size_t count) {
    Timer t;

    Allocate();
    Emplace(count);

    t.Resume();
    if constexpr(IS_SHM_ARCHIVEABLE(MapT)) {
      volatile auto vec2 = hipc::make_uptr<MapT>(std::move(*map_));
    } else {
      volatile MapT vec2(*map_);
    }
    t.Pause();

    TestOutput("Move", t);
    Destroy();
  }

 private:
  /**====================================
   * Helpers
   * ===================================*/

  /** Output as CSV */
  void TestOutput(const std::string &test_name, Timer &t) {
    HIPRINT("{},{},{},{}\n",
            test_name, map_type_, internal_type_, t.GetMsec())
  }

  /** Get element at position i */
  void Get(size_t i) {
    if constexpr(std::is_same_v<MapT, std::unordered_map<size_t, T>>) {
      T &x = (*map_)[i];
      USE(x);
    } else if constexpr(std::is_same_v<MapT, bipc_unordered_map<size_t, T>>) {
      auto iter = (*map_).find(i);
      USE(*iter);
    } else if constexpr(std::is_same_v<MapT, hipc::unordered_map<size_t, T>>) {
      T &x = (*map_)[i];
      USE(x);
    }
  }

  /** Emplace elements into the unordered_map */
  void Emplace(size_t count) {
    StringOrInt<T> var(124);
    for (size_t i = 0; i < count; ++i) {
      if constexpr(std::is_same_v<MapT, std::unordered_map<size_t, T>>) {
        map_->emplace(i, var.Get());
      } else if constexpr(std::is_same_v<MapT, bipc_unordered_map<size_t, T>>) {
        map_->emplace(i, var.Get());
      } else if constexpr(std::is_same_v<MapT, hipc::unordered_map<size_t, T>>) {
        map_->emplace(i, var.Get());
      }
    }
  }

  /** Allocate an arbitrary unordered_map for the test cases */
  void Allocate() {
    if constexpr(std::is_same_v<MapT, hipc::unordered_map<size_t, T>>) {
      map_ptr_ = hipc::make_mptr<MapT>(5000);
      map_ = map_ptr_.get();
    } else if constexpr(std::is_same_v<MapT, std::unordered_map<size_t, T>>) {
      map_ptr_ = new std::unordered_map<size_t, T>();
      map_ = map_ptr_;
    } else if constexpr (std::is_same_v<MapT, bipc_unordered_map<size_t, T>>) {
      map_ptr_ = BOOST_SEGMENT->construct<MapT>("BoostMap")(
        BOOST_ALLOCATOR((std::pair<size_t, T>)));
      map_ = map_ptr_;
    }
  }

  /** Destroy the unordered_map */
  void Destroy() {
    if constexpr(std::is_same_v<MapT, hipc::unordered_map<size_t, T>>) {
      map_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<MapT, std::unordered_map<size_t, T>>) {
      delete map_ptr_;
    } else if constexpr (std::is_same_v<MapT, bipc_unordered_map<size_t, T>>) {
      BOOST_SEGMENT->destroy<MapT>("BoostMap");
    }
  }
};

void FullUnorderedMapTest() {
  // std::unordered_map tests
  UnorderedMapTest<size_t, std::unordered_map<size_t, size_t>>().Test();
  UnorderedMapTest<std::string, std::unordered_map<size_t, std::string>>().Test();

  // boost::unordered_map tests
  UnorderedMapTest<size_t, bipc_unordered_map<size_t, size_t>>().Test();
  UnorderedMapTest<std::string, bipc_unordered_map<size_t, std::string>>().Test();
  UnorderedMapTest<bipc_string, bipc_unordered_map<size_t, bipc_string>>().Test();

  // hipc::unordered_map tests
  UnorderedMapTest<size_t, hipc::unordered_map<size_t, size_t>>().Test();
  UnorderedMapTest<std::string, hipc::unordered_map<size_t, std::string>>().Test();
  UnorderedMapTest<hipc::string, hipc::unordered_map<size_t, hipc::string>>().Test();
}

TEST_CASE("UnorderedMapBenchmark") {
  FullUnorderedMapTest();
}
