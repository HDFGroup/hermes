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

#include <string>
#include "hermes_shm/data_structures/ipc/string.h"

template<typename T>
class StringTestSuite {
 public:
  std::string str_type_;
  void *ptr_;

  /**====================================
   * Test Cases
   * ===================================*/

  /** Constructor */
  StringTestSuite() {
    if constexpr(std::is_same_v<std::string, T>) {
      str_type_ = "std::string";
    } else if constexpr(std::is_same_v<hipc::string, T>) {
      str_type_ = "hipc::string";
    } else if constexpr(std::is_same_v<bipc_string, T>) {
      str_type_ = "bipc::string";
    }
  }

  /** Dereference performance */
  void ConstructDestructTest(size_t count, int length) {
    std::string data(length, 1);

    Timer t;
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      if constexpr(std::is_same_v<std::string, T>) {
        T hello(data);
        USE(hello);
      } else if constexpr(std::is_same_v<hipc::string, T>) {
        auto hello = hipc::make_uptr<hipc::string>(data);
        USE(hello);
      } else if constexpr(std::is_same_v<bipc_string, T>) {
        auto hello = BOOST_SEGMENT->find_or_construct<bipc_string>("MyString")(
          BOOST_ALLOCATOR(bipc_string));
        BOOST_SEGMENT->destroy<bipc_string>("MyString");
        USE(hello);
      }
    }
    t.Pause();

    TestOutput("ConstructDestructTest", t, length);
  }

  /** Deserialize a string in a loop */
  void DeserializeTest(size_t count, int length) {
    std::string data(length, 1);
    std::string *test1 = &data;
    auto test2 = hipc::make_uptr<hipc::string>(data);
    bipc_string *test3 = BOOST_SEGMENT->find_or_construct<bipc_string>("MyString")(
      BOOST_ALLOCATOR(bipc_string));
    test3->assign(data);

    Timer t;
    t.Resume();
    for (size_t i = 0; i < count; ++i) {
      if constexpr(std::is_same_v<std::string, T>) {
        auto info = test1->data();
        USE(info);
      } else if constexpr(std::is_same_v<hipc::string, T>) {
        auto info = test2->data();
        USE(info);
      } else if constexpr(std::is_same_v<bipc_string, T>) {
        auto info = test3->c_str();
        USE(info);
      }
    }
    t.Pause();

    TestOutput("DeserializeTest", t, length);
  }

  /**====================================
   * Test Output
   * ===================================*/

  /** Output test results */
  void TestOutput(const std::string &test_name, Timer &t, size_t length) {
    HIPRINT("{},{},{},{}\n",
            test_name, str_type_, length, t.GetMsec())
  }
};

template<typename T>
void StringTest() {
  size_t count = 100000;
  StringTestSuite<T>().ConstructDestructTest(count, 16);
  StringTestSuite<T>().ConstructDestructTest(count, 256);
  StringTestSuite<T>().DeserializeTest(count, 16);
}

void FullStringTest() {
  StringTest<std::string>();
  StringTest<hipc::string>();
  StringTest<bipc_string>();
}

TEST_CASE("StringBenchmark") {
  FullStringTest();
}
