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
#include <boost/interprocess/containers/list.hpp>

// Std
#include <string>
#include <list>

// hermes
#include "hermes_shm/data_structures/ipc/string.h"
#include <hermes_shm/data_structures/ipc/list.h>
#include <hermes_shm/data_structures/ipc/slist.h>

template<typename T>
using bipc_list = bipc::list<T, typename BoostAllocator<T>::alloc_t>;

/**
 * A series of performance tests for vectors
 * OUTPUT:
 * [test_name] [vec_type] [internal_type] [time_ms]
 * */
template<typename T, typename ListT,
  typename ListTPtr=SHM_X_OR_Y(ListT, hipc::mptr<ListT>, ListT*)>
class ListTest {
 public:
  std::string list_type_;
  std::string internal_type_;
  ListT *lp_;
  ListTPtr list_ptr_;
  void *ptr_;

  /**====================================
   * Test Runner
   * ===================================*/

  /** Test case constructor */
  ListTest() {
    if constexpr(std::is_same_v<std::list<T>, ListT>) {
      list_type_ = "std::list";
    } else if constexpr(std::is_same_v<hipc::list<T>, ListT>) {
      list_type_ = "hipc::list";
    } else if constexpr(std::is_same_v<bipc_list<T>, ListT>) {
      list_type_ = "bipc_list";
    } else if constexpr(std::is_same_v<hipc::slist<T>, ListT>) {
      list_type_ = "hipc::slist";
    } else {
      HELOG(kFatal, "none of the list tests matched")
    }
    internal_type_ = InternalTypeName<T>::Get();
  }

  /** Run the tests */
  void Test() {
    size_t count = 10000;
    AllocateTest(count);
    EmplaceTest(count);
    ForwardIteratorTest(count);
    CopyTest(count);
    MoveTest(count);
  }

  /**====================================
   * Tests
   * ===================================*/

  /** Test performance of list allocation */
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

  /** Emplace after reserving enough space */
  void EmplaceTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    t.Resume();
    Emplace(count);
    t.Pause();

    TestOutput("FixedEmplace", t);
    Destroy();
  }

  /** Iterator performance */
  void ForwardIteratorTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    Emplace(count);

    t.Resume();
    int i = 0;
    for (auto &x : *lp_) {
      USE(x);
      ++i;
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
    if constexpr(IS_SHM_ARCHIVEABLE(ListT)) {
      auto vec2 = hipc::make_uptr<ListT>(*lp_);
      USE(vec2);
    } else {
      ListT vec2(*lp_);
      USE(vec2);
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
    if constexpr(IS_SHM_ARCHIVEABLE(ListT)) {
      auto vec2 = hipc::make_uptr<ListT>(std::move(*lp_));
      USE(vec2)
    } else {
      ListT vec2(*lp_);
      USE(vec2)
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
            test_name, list_type_, internal_type_, t.GetMsec())
  }

  /** Get element at position i */
  void Get(size_t i) {
    if constexpr(std::is_same_v<ListT, std::list<T>>) {
      T &x = (*lp_)[i];
      USE(x);
    } else if constexpr(std::is_same_v<ListT, bipc_list<T>>) {
      T &x = (*lp_)[i];
      USE(x);
    } else if constexpr(std::is_same_v<ListT, hipc::list<T>>) {
      T &x = (*lp_)[i];
      USE(x);
    } else if constexpr(std::is_same_v<ListT, hipc::slist<T>>) {
      T &x = (*lp_)[i];
      USE(x);
    }
  }

  /** Emplace elements into the list */
  void Emplace(size_t count) {
    StringOrInt<T> var(124);
    for (size_t i = 0; i < count; ++i) {
      if constexpr(std::is_same_v<ListT, std::list<T>>) {
        lp_->emplace_back(var.Get());
      } else if constexpr(std::is_same_v<ListT, bipc_list<T>>) {
        lp_->emplace_back(var.Get());
      } else if constexpr(std::is_same_v<ListT, hipc::list<T>>) {
        lp_->emplace_back(var.Get());
      } else if constexpr(std::is_same_v<ListT, hipc::slist<T>>) {
        lp_->emplace_back(var.Get());
      }
    }
  }

  /** Allocate an arbitrary list for the test cases */
  void Allocate() {
    if constexpr(std::is_same_v<ListT, hipc::list<T>>) {
      list_ptr_ = hipc::make_mptr<ListT>();
      lp_ = list_ptr_.get();
    } if constexpr(std::is_same_v<ListT, hipc::slist<T>>) {
      list_ptr_ = hipc::make_mptr<ListT>();
      lp_ = list_ptr_.get();
    } else if constexpr (std::is_same_v<ListT, bipc_list<T>>) {
      list_ptr_ = BOOST_SEGMENT->construct<ListT>("BoostList")(
        BOOST_ALLOCATOR((std::pair<int, T>)));
      lp_ = list_ptr_;
    } else if constexpr(std::is_same_v<ListT, std::list<T>>) {
      list_ptr_ = new std::list<T>();
      lp_ = list_ptr_;
    }
  }

  /** Destroy the list */
  void Destroy() {
    if constexpr(std::is_same_v<ListT, hipc::list<T>>) {
      list_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<ListT, hipc::slist<T>>) {
      list_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<ListT, std::list<T>>) {
      delete list_ptr_;
    } else if constexpr (std::is_same_v<ListT, bipc_list<T>>) {
      BOOST_SEGMENT->destroy<ListT>("BoostList");
    }
  }
};

void FullListTest() {
  // std::list tests
  ListTest<size_t, std::list<size_t>>().Test();
  ListTest<std::string, std::list<std::string>>().Test();

  // boost::ipc::list tests
  ListTest<size_t, bipc_list<size_t>>().Test();
  ListTest<std::string, bipc_list<std::string>>().Test();
  ListTest<bipc_string, bipc_list<bipc_string>>().Test();

  // hipc::list tests
  ListTest<size_t, hipc::list<size_t>>().Test();
  ListTest<std::string, hipc::list<std::string>>().Test();
  ListTest<hipc::string, hipc::list<hipc::string>>().Test();

  // hipc::slist tests
  ListTest<size_t, hipc::slist<size_t>>().Test();
  ListTest<std::string, hipc::slist<std::string>>().Test();
  ListTest<hipc::string, hipc::slist<hipc::string>>().Test();
}

TEST_CASE("ListBenchmark") {
  FullListTest();
}
