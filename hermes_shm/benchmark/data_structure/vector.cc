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

// Boost interprocess
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/string.hpp>
#include <boost/interprocess/containers/vector.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/interprocess/allocators/allocator.hpp>

// Boost private
#include <boost/container/scoped_allocator.hpp>
#include <boost/container/string.hpp>
#include <boost/container/vector.hpp>

// Std
#include <string>
#include <vector>

// hermes
#include <hermes_shm/data_structures/string.h>
#include <hermes_shm/data_structures/thread_unsafe/vector.h>

#include "basic_test.h"
#include "test_init.h"

namespace bipc = boost::interprocess;

#define SET_VAR_TO_INT_OR_STRING(TYPE, VAR, VAL)\
  if constexpr(std::is_same_v<TYPE, hipc::string>) {\
    VAR = hipc::string(std::to_string(VAL));\
  } else if constexpr(std::is_same_v<TYPE, std::string>) {\
    VAR = std::string(std::to_string(VAL));\
  } else {\
    VAR = VAL;\
  }

/**
 * A series of performance tests for vectors
 * OUTPUT:
 * [test_name] [vec_type] [internal_type] [time_ms]
 * */
template<typename T, typename VecT>
class VectorTest {
 public:
  std::string vec_type;
  std::string internal_type;
  VecT *vec;

  VectorTest() {
    if constexpr(std::is_same_v<std::vector<T>, VecT>) {
      vec = new VecT();
      vec_type = "std::vector";
    } else if constexpr(std::is_same_v<hipc::vector<T>, VecT>) {
      vec = new VecT();
      vec_type = "hipc::vector";
    } else if constexpr(std::is_same_v<boost::container::vector<T>, VecT>) {
      vec = new VecT();
      vec_type = "boost::vector";
    } else if constexpr(std::is_same_v<bipc::vector<T>, VecT>) {
      vec = BoostIpcVector();
      vec_type = "bipc::vector";
    } else {
      std::cout << "INVALID: none of the vector tests matched" << std::endl;
      return;
    }

    if constexpr(std::is_same_v<T, hipc::string>) {
      internal_type = "hipc::string";
    } else if constexpr(std::is_same_v<T, std::string>) {
      internal_type = "std::string";
    } else if constexpr(std::is_same_v<T, int>) {
      internal_type = "int";
    }
  }

  void TestOutput(const std::string &test_name, Timer &t) {
    printf("%s, %s, %s, %lf\n",
           test_name.c_str(),
           vec_type.c_str(),
           internal_type.c_str(),
           t.GetMsec());
  }

  void ResizeTest(VecT &vec, int count) {
    Timer t;
    T var;

    SET_VAR_TO_INT_OR_STRING(T, var, 124);

    t.Resume();
    vec.resize(count);
    t.Pause();

    TestOutput("FixedResize", t);
  }

  void ReserveEmplaceTest(VecT &vec, int count) {
    Timer t;
    T var;
    SET_VAR_TO_INT_OR_STRING(T, var, 124);

    t.Resume();
    vec.reserve(count);
    for (int i = 0; i < count; ++i) {
      vec.emplace_back(var);
    }
    t.Pause();

    TestOutput("FixedEmplace", t);
  }

  void GetTest(VecT &vec, int count) {
    Timer t;
    T var;
    SET_VAR_TO_INT_OR_STRING(T, var, 124);

    vec.reserve(count);
    for (int i = 0; i < count; ++i) {
      vec.emplace_back(var);
    }

    t.Resume();
    for (int i = 0; i < count; ++i) {
      auto x = vec[i];
    }
    t.Pause();

    TestOutput("FixedGet", t);
  }

  void ForwardIteratorTest(VecT &vec, int count) {
    Timer t;
    T var;
    SET_VAR_TO_INT_OR_STRING(T, var, 124);

    vec.reserve(count);
    for (int i = 0; i < count; ++i) {
      vec.emplace_back(var);
    }

    t.Resume();
    int i = 0;
    for (auto x : vec) {
      ++i;
    }
    t.Pause();

    TestOutput("ForwardIterator", t);
  }

  void CopyTest(VecT &vec, int count) {
    Timer t;
    T var;
    SET_VAR_TO_INT_OR_STRING(T, var, 124);

    vec.reserve(count);
    for (int i = 0; i < count; ++i) {
      vec.emplace_back(var);
    }

    t.Resume();
    VecT vec2(vec);
    t.Pause();

    TestOutput("Copy", t);
  }

  void MoveTest(VecT &vec, int count) {
    Timer t;
    T var;
    SET_VAR_TO_INT_OR_STRING(T, var, 124);

    vec.reserve(count);
    for (int i = 0; i < count; ++i) {
      vec.emplace_back(var);
    }

    t.Resume();
    VecT vec2(std::move(vec));
    t.Pause();

    TestOutput("Move", t);
  }

  VecT *BoostIpcVector() {
    void_allocator &alloc_inst = *alloc_inst_g;
    bipc::managed_shared_memory &segment = *segment_g;
    VecT *vec = segment.construct<VecT>("BoostVector")(alloc_inst);
    return vec;
  }

  void Test() {
    ResizeTest(*vec, 1000000);
    ReserveEmplaceTest(*vec, 1000000);
    GetTest(*vec, 1000000);
    ForwardIteratorTest(*vec, 1000000);
    CopyTest(*vec, 1000000);
    MoveTest(*vec, 1000000);
  }
};

void FullVectorTest() {
  // std::vector tests
  VectorTest<int, std::vector<int>>().Test();
  // VectorTest<std::string, std::vector<std::string>>().Test();
  // VectorTest<hipc::string, std::vector<hipc::string>>().Test();

  // boost::vector tests
//  VectorTest<int, boost::container::vector<int>>().Test();
//  VectorTest<std::string, boost::container::vector<std::string>>().Test();
//  VectorTest<hipc::string, boost::container::vector<hipc::string>>().Test();

  // boost::ipc::vector tests
  // VectorTest<int, bipc::vector<int>>().Test();
  // VectorTest<std::string, bipc::vector<std::string>>().Test();
  // VectorTest<hipc::string, bipc::vector<hipc::string>>().Test();

  // hipc::vector tests
  VectorTest<int, hipc::vector<int>>().Test();
  // VectorTest<std::string, hipc::vector<std::string>>().Test();
  // VectorTest<hipc::string, hipc::vector<hipc::string>>().Test();
}

TEST_CASE("VectorBenchmark") {
  FullVectorTest();
}
