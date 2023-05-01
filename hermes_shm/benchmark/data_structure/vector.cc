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
#include <boost/interprocess/containers/vector.hpp>

// Std
#include <string>
#include <vector>

// hermes
#include "hermes_shm/data_structures/ipc/string.h"
#include <hermes_shm/data_structures/ipc/vector.h>

template<typename T>
using bipc_vector = bipc::vector<T, typename BoostAllocator<T>::alloc_t>;

/**
 * A series of performance tests for vectors
 * OUTPUT:
 * [test_name] [vec_type] [internal_type] [time_ms]
 * */
template<typename T, typename VecT,
  typename VecTPtr=SHM_X_OR_Y(VecT, hipc::mptr<VecT>, VecT*)>
class VectorTest {
 public:
  std::string vec_type_;
  std::string internal_type_;
  VecT *vec_;
  VecTPtr vec_ptr_;
  void *ptr_;

  /**====================================
   * Test Runner
   * ===================================*/

  /** Test case constructor */
  VectorTest() {
    if constexpr(std::is_same_v<std::vector<T>, VecT>) {
      vec_type_ = "std::vector";
    } else if constexpr(std::is_same_v<hipc::vector<T>, VecT>) {
      vec_type_ = "hipc::vector";
    } else if constexpr(std::is_same_v<bipc_vector<T>, VecT>) {
      vec_type_ = "bipc_vector";
    } else {
      std::cout << "INVALID: none of the vector tests matched" << std::endl;
      return;
    }
    internal_type_ = InternalTypeName<T>::Get();
  }

  /** Run the tests */
  void Test() {
    size_t count = 1000000;
    // AllocateTest(count);
    // ResizeTest(count);
    ReserveEmplaceTest(count);
    GetTest(count);
    BeginIteratorTest(count);
    EndIteratorTest(count);
    ForwardIteratorTest(count);
    // CopyTest(count);
    // MoveTest(count);
  }

  /**====================================
   * Tests
   * ===================================*/

  /** Test performance of vector allocation */
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

  /** Test the performance of a resize */
  void ResizeTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    t.Resume();
    vec_->resize(count);
    t.Pause();

    TestOutput("FixedResize", t);
    Destroy();
  }

  /** Emplace after reserving enough space */
  void ReserveEmplaceTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    t.Resume();
    vec_->reserve(count);
    Emplace(count);
    t.Pause();

    TestOutput("FixedEmplace", t);
    Destroy();
  }

  /** Get performance */
  void GetTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    vec_->reserve(count);
    Emplace(count);

    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      Get(i);
    }
    t.Pause();

    TestOutput("FixedGet", t);
    Destroy();
  }

  /** Begin iterator performance */
  void BeginIteratorTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    vec_->reserve(count);
    Emplace(count);

    t.Resume();
    if constexpr(IS_SHM_ARCHIVEABLE(VecT)) {
      auto iter = vec_->begin();
      USE(iter);
    } else {
      auto iter = vec_->begin();
      USE(iter);
    }
    t.Pause();

    TestOutput("BeginIterator", t);
    Destroy();
  }

  /** End iterator performance */
  void EndIteratorTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    vec_->reserve(count);
    Emplace(count);

    t.Resume();
    if constexpr(IS_SHM_ARCHIVEABLE(VecT)) {
      auto iter = vec_->end();
      USE(iter);
    } else {
      auto iter = vec_->end();
      USE(iter);
    }
    t.Pause();

    TestOutput("EndIterator", t);
    Destroy();
  }

  /** Iterator performance */
  void ForwardIteratorTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    vec_->reserve(count);
    Emplace(count);

    t.Resume();
    int i = 0;
    for (auto &x : *vec_) {
      USE(x);
      ++i;
    }
    t.Pause();

    if (ptr_ == nullptr) {
      std::cout << "hm" << std::endl;
    }

    TestOutput("ForwardIterator", t);
    Destroy();
  }

  /** Copy performance */
  void CopyTest(size_t count) {
    Timer t;
    StringOrInt<T> var(124);

    Allocate();
    vec_->reserve(count);
    Emplace(count);

    t.Resume();
    if constexpr(IS_SHM_ARCHIVEABLE(VecT)) {
      auto vec2 = hipc::make_uptr<VecT>(*vec_);
      USE(vec2);
    } else {
      VecT vec2(*vec_);
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
    vec_->reserve(count);
    Emplace(count);

    t.Resume();
    if constexpr(IS_SHM_ARCHIVEABLE(VecT)) {
      auto vec2 = hipc::make_uptr<VecT>(std::move(*vec_));
      USE(vec2)
    } else {
      VecT vec2(*vec_);
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
            test_name, vec_type_, internal_type_, t.GetMsec())
  }

  /** Get element at position i */
  void Get(size_t i) {
    if constexpr(std::is_same_v<std::vector<T>, VecT>) {
      T &x = (*vec_)[i];
      USE(x);
    } else if constexpr(std::is_same_v<hipc::vector<T>, VecT>) {
      T &x = (*vec_)[i];
      USE(x);
    } else if constexpr(std::is_same_v<bipc_vector<T>, VecT>) {
      T &x = (*vec_)[i];
      USE(x);
      if (!ptr_) { std::cout << "h" << std::endl; }
    }
  }

  /** Emplace elements into the vector */
  void Emplace(size_t count) {
    StringOrInt<T> var(124);
    for (size_t i = 0; i < count; ++i) {
      if constexpr(std::is_same_v<std::vector<T>, VecT>) {
        vec_->emplace_back(var.Get());
      } else if constexpr(std::is_same_v<hipc::vector<T>, VecT>) {
        vec_->emplace_back(var.Get());
      } else if constexpr(std::is_same_v<bipc_vector<T>, VecT>) {
        vec_->emplace_back(var.Get());
      }
    }
  }

  /** Allocate an arbitrary vector for the test cases */
  void Allocate() {
    if constexpr(std::is_same_v<std::vector<T>, VecT>) {
      vec_ptr_ = new std::vector<T>();
      vec_ = vec_ptr_;
    } else if constexpr(std::is_same_v<hipc::vector<T>, VecT>) {
      vec_ptr_ = hipc::make_mptr<VecT>();
      vec_ = vec_ptr_.get();
    } else if constexpr(std::is_same_v<bipc_vector<T>, VecT>) {
      vec_ptr_ = BOOST_SEGMENT->construct<VecT>("BoostVector")(
        BOOST_ALLOCATOR((std::pair<int, T>)));
      vec_ = vec_ptr_;
    }
  }

  /** Destroy the vector */
  void Destroy() {
    if constexpr(std::is_same_v<std::vector<T>, VecT>) {
      delete vec_ptr_;
    } else if constexpr(std::is_same_v<hipc::vector<T>, VecT>) {
      vec_ptr_.shm_destroy();
    } else if constexpr(std::is_same_v<bipc_vector<T>, VecT>) {
      BOOST_SEGMENT->destroy<VecT>("BoostVector");
    }
  }
};

void FullVectorTest() {
  // std::vector tests
  VectorTest<size_t, std::vector<size_t>>().Test();
  VectorTest<std::string, std::vector<std::string>>().Test();

  // boost::ipc::vector tests
  VectorTest<size_t, bipc_vector<size_t>>().Test();
  VectorTest<std::string, bipc_vector<std::string>>().Test();
  VectorTest<bipc_string, bipc_vector<bipc_string>>().Test();

  // hipc::vector tests
  VectorTest<size_t, hipc::vector<size_t>>().Test();
  VectorTest<std::string, hipc::vector<std::string>>().Test();
  VectorTest<hipc::string, hipc::vector<hipc::string>>().Test();
}

TEST_CASE("VectorBenchmark") {
  FullVectorTest();
}
