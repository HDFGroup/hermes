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

#ifndef HERMES_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_IQUEUE_H_
#define HERMES_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_IQUEUE_H_

#include "basic_test.h"
#include "test_init.h"
#include <hermes_shm/memory/allocator/mp_page.h>

using hipc::MpPage;

template<typename T, typename Container>
class IqueueTestSuite {
 public:
  Container &obj_;
  Allocator *alloc_;

  /// Constructor
  IqueueTestSuite(Container &obj, Allocator *alloc)
  : obj_(obj), alloc_(alloc) {}

  /// Enqueue elements
  void EnqueueTest(size_t count = 30) {
    for (size_t i = 0; i < count; ++i) {
      hipc::OffsetPointer p;
      auto page = alloc_->template
        AllocateConstructObjs<T>(1, p);
      page->page_size_ = count - i - 1;
      obj_.enqueue(page);
    }
    REQUIRE(obj_.size() == count);
  }

  /// Dequeue and then re-enqueue
  void DequeueTest(size_t count = 30) {
    std::vector<T*> tmp(count);
    for (size_t i = 0; i < count; ++i) {
      tmp[i] = obj_.dequeue();
    }
    for (int i = count - 1; i >= 0; --i) {
      obj_.enqueue(tmp[i]);
    }
    REQUIRE(obj_.size() == count);
    ForwardIteratorTest(count);
  }

  /// Forward iterator
  void ForwardIteratorTest(size_t count = 30) {
    size_t fcur = 0;
    for (T *page : obj_) {
      REQUIRE(page->page_size_ == fcur);
      ++fcur;
    }
  }

  /// Constant Forward iterator
  void ConstForwardIteratorTest(size_t count = 30) {
    const Container &obj = obj_;
    size_t fcur = 0;
    for (auto iter = obj.cbegin(); iter != obj.cend(); ++iter) {
      T *page = *iter;
      REQUIRE(page->page_size_ == fcur);
      ++fcur;
    }
  }

  /// Dequeue an element in the middle of the queue
  void DequeueMiddleTest() {
    size_t count = obj_.size();
    size_t mid = obj_.size() / 2;
    auto iter = obj_.begin();
    for (size_t i = 0; i < mid; ++i) {
      ++iter;
    }
    auto page = obj_.dequeue(iter);
    REQUIRE(page->page_size_ == mid);
    REQUIRE(obj_.size() == count - 1);
    for (T *page : obj_) {
      REQUIRE(page->page_size_ != mid);
    }
    obj_.enqueue(page);
  }

  /// Verify erase
  void EraseTest() {
    std::vector<T*> tmp;
    for (T *page : obj_) {
      tmp.emplace_back(page);
    }
    obj_.clear();
    for (T *page : tmp) {
      alloc_->FreePtr(page);
    }
    REQUIRE(obj_.size() == 0);
  }
};

#endif  // HERMES_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_IQUEUE_H_
