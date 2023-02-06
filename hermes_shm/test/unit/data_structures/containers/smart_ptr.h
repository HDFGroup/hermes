//
// Created by lukemartinlogan on 1/20/23.
//

#ifndef HERMES_SHM_TEST_UNIT_ptr__STRUCTURES_CONTAINERS_SMART_PTR_H_
#define HERMES_SHM_TEST_UNIT_ptr__STRUCTURES_CONTAINERS_SMART_PTR_H_

#include "basic_test.h"
#include "test_init.h"

namespace hermes_shm::ipc {

template<typename T, typename PointerT>
class SmartPtrTestSuite {
 public:
  PointerT ptr_;

 public:
  // Test dereference
  void DereferenceTest(T &num) {
    REQUIRE(ptr_.get_ref() == num);
    REQUIRE(ptr_.get_ref_const() == num);
    REQUIRE(*ptr_ == num);
  }

  // Test move constructor
  void MoveConstructorTest(T &num) {
    PointerT ptr2(std::move(ptr_));
    REQUIRE(ptr_.IsNull());
    REQUIRE(std::hash<PointerT>{}(ptr2) == std::hash<T>{}(num));
    ptr_ = std::move(ptr2);
  }

  // Test move assignment operator
  void MoveAssignmentTest(T &num) {
    PointerT ptr2 = std::move(ptr_);
    REQUIRE(ptr_.IsNull());
    REQUIRE(std::hash<PointerT>{}(ptr2) == std::hash<T>{}(num));
    ptr_ = std::move(ptr2);
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

}  // namespace hermes_shm::ipc

#endif //HERMES_SHM_TEST_UNIT_ptr__STRUCTURES_CONTAINERS_SMART_PTR_H_
