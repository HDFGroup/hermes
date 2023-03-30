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

#ifndef HERMES_TEST_UNIT_ptr__STRUCTURES_CONTAINERS_SMART_PTR_H_
#define HERMES_TEST_UNIT_ptr__STRUCTURES_CONTAINERS_SMART_PTR_H_

#include "basic_test.h"
#include "test_init.h"

namespace hshm::ipc {

template<typename T, typename PointerT>
class SmartPtrTestSuite {
 public:
  PointerT &ptr_;
  PointerT &ptr2_;

  explicit SmartPtrTestSuite(PointerT &ptr,
                             PointerT &ptr2) : ptr_(ptr), ptr2_(ptr2) {}

 public:
  // Test dereference
  void DereferenceTest(T &num) {
    REQUIRE(*ptr_ == num);
  }

  // Test move constructor
  void MoveConstructorTest(T &num) {
    PointerT ptr2(std::move(ptr_));
    // REQUIRE(ptr_.IsNull());
    REQUIRE(std::hash<PointerT>{}(ptr2) == std::hash<T>{}(num));
    ptr_ = std::move(ptr2);
  }

  // Test move assignment operator
  void MoveAssignmentTest(T &num) {
    ptr2_ = std::move(ptr_);
    // REQUIRE(ptr_.IsNull());
    REQUIRE(std::hash<PointerT>{}(ptr2_) == std::hash<T>{}(num));
    ptr_ = std::move(ptr2_);
  }

  // Test copy constructor
  void CopyConstructorTest(T &num) {
    PointerT ptr2(ptr_);
    REQUIRE(*ptr_ == num);
    REQUIRE(*ptr2 == num);
  }

  // Test copy assignment
  void CopyAssignmentTest(T &num) {
    PointerT ptr2 = ptr_;
    REQUIRE(*ptr_ == num);
    REQUIRE(*ptr2 == num);
  }

  // Test serialization + deserialization (constructor)
  void SerializeationConstructorTest(T &num) {
    TypedPointer<T> ar;
    ptr_ >> ar;
    PointerT from_ar(ar);
    REQUIRE(*from_ar == num);
  }

  // Test serialization + deserialization (operator)
  void SerializeationOperatorTest(T &num) {
    TypedPointer<T> ar;
    ptr_ >> ar;
    PointerT from_ar;
    from_ar << ar;
    REQUIRE(*from_ar == num);
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_TEST_UNIT_ptr__STRUCTURES_CONTAINERS_SMART_PTR_H_
