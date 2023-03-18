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
#include "hermes_shm/data_structures/containers/tuple_base.h"
#include <utility>

void test_argpack0_pass() {
  std::cout << "HERE0" << std::endl;
}

void test_argpack0() {
  hermes_shm::PassArgPack::Call(hermes_shm::ArgPack<>(), test_argpack0_pass);
}

template<typename T1, typename T2, typename T3>
void test_argpack3_pass(T1 x, T2 y, T3 z) {
  REQUIRE(x == 0);
  REQUIRE(y == 1);
  REQUIRE(z == 0);
  std::cout << "HERE3" << std::endl;
}

void test_product1(int b, int c) {
  REQUIRE(b == 1);
  REQUIRE(c == 2);
}

void test_product2(double d, double e) {
  REQUIRE(d == 3);
  REQUIRE(e == 4);
}

template<typename Pack1, typename Pack2>
void test_product(int a, Pack1 &&pack1, int a2, Pack2 &&pack2) {
  REQUIRE(a == 0);
  REQUIRE(a2 == 0);
  hermes_shm::PassArgPack::Call(
      std::forward<Pack1>(pack1),
      test_product1);
  hermes_shm::PassArgPack::Call(
      std::forward<Pack2>(pack2),
      test_product2);
}

template<typename T1, typename T2, typename T3>
void verify_tuple3(hermes_shm::tuple<T1, T2, T3> &x) {
  REQUIRE(x.Size() == 3);
  REQUIRE(x.template Get<0>() == 0);
  REQUIRE(x.template Get<1>() == 1);
  REQUIRE(x.template Get<2>() == 0);
#ifdef TEST_COMPILER_ERROR
  std::cout << x.Get<3>() << std::endl;
#endif
}

template<typename T1, typename T2, typename T3>
void test_argpack3() {
  // Pass an argpack to a function
  PAGE_DIVIDE("") {
    hermes_shm::PassArgPack::Call(
        hermes_shm::make_argpack(T1(0), T2(1), T3(0)),
        test_argpack3_pass<T1, T2, T3>);
  }

  // Pass an argpack containing references to a function
  PAGE_DIVIDE("") {
    T2 y = 1;
    hermes_shm::PassArgPack::Call(
        hermes_shm::make_argpack(0, y, 0),
        test_argpack3_pass<T1, T2, T3>);
  }

  // Create a 3-tuple
  PAGE_DIVIDE("") {
    hermes_shm::tuple<T1, T2, T3> x(0, 1, 0);
    verify_tuple3(x);
  }

  // Copy a tuple
  PAGE_DIVIDE("") {
    hermes_shm::tuple<T1, T2, T3> y(0, 1, 0);
    hermes_shm::tuple<T1, T2, T3> x(y);
    verify_tuple3(x);
  }

  // Copy assign tuple
  PAGE_DIVIDE("") {
    hermes_shm::tuple<T1, T2, T3> y(0, 1, 0);
    hermes_shm::tuple<T1, T2, T3> x;
    x = y;
    verify_tuple3(x);
  }

  // Move tuple
  PAGE_DIVIDE("") {
    hermes_shm::tuple<T1, T2, T3> y(0, 1, 0);
    hermes_shm::tuple<T1, T2, T3> x(std::move(y));
    verify_tuple3(x);
  }

  // Move assign tuple
  PAGE_DIVIDE("") {
    hermes_shm::tuple<T1, T2, T3> y(0, 1, 0);
    hermes_shm::tuple<T1, T2, T3> x;
    x = std::move(y);
    verify_tuple3(x);
  }

  // Iterate over a tuple
  PAGE_DIVIDE("") {
    hermes_shm::tuple<T1, T2, T3> x(0, 1, 0);
    hermes_shm::ForwardIterateTuple::Apply(
        x,
        [](auto i, auto &arg) constexpr {
          std::cout << "lambda: " << i.Get() << std::endl;
        });
  }

  // Merge two argpacks into a single pack
  PAGE_DIVIDE("") {
    size_t y = hermes_shm::MergeArgPacks::Merge(
                   hermes_shm::make_argpack(T1(0)),
                   hermes_shm::make_argpack(T2(1), T2(0))).Size();
    REQUIRE(y == 3);
  }

  // Pass a merged argpack to a function
  PAGE_DIVIDE("") {
    hermes_shm::PassArgPack::Call(
        hermes_shm::MergeArgPacks::Merge(
            hermes_shm::make_argpack(0),
            hermes_shm::make_argpack(1, 0)),
        test_argpack3_pass<T1, T2, T3>);
  }

  // Construct tuple from argpack
  PAGE_DIVIDE("") {
    hermes_shm::tuple<int, int, int> x(
        hermes_shm::make_argpack(10, 11, 12));
    REQUIRE(x.Get<0>() == 10);
    REQUIRE(x.Get<1>() == 11);
    REQUIRE(x.Get<2>() == 12);
  }

  // Product an argpack
  PAGE_DIVIDE("") {
    auto&& pack = hermes_shm::ProductArgPacks::Product(
        0,
        hermes_shm::make_argpack(1, 2),
        hermes_shm::make_argpack<double, double>(3, 4));
    REQUIRE(pack.Size() == 4);
  }

  // Product an argpack
  PAGE_DIVIDE("") {
    hermes_shm::PassArgPack::Call(
        hermes_shm::ProductArgPacks::Product(
            0,
            hermes_shm::make_argpack(1, 2),
            hermes_shm::make_argpack(3.0, 4.0)),
        test_product<
            hermes_shm::ArgPack<int&&, int&&>,
            hermes_shm::ArgPack<double&&, double&&>>);
  }
}

TEST_CASE("TestArgpack") {
  test_argpack0();
  test_argpack3<int, double, float>();
}
