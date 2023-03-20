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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_NUMA_AWARE_LIST_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_NUMA_AWARE_LIST_H_

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/data_structures/ipc/list.h"
#include "hermes_shm/data_structures/ipc/vector.h"

namespace hermes_shm::ipc {

template<typename T>
class numa_list;

#define CLASS_NAME numa_list
#define TYPED_CLASS numa_list
#define TYPED_HEADER ShmHeader<numa_list>

template<typename T>
class ShmHeader;

template<typename T>
class ShmHeader<numa_list<T>> : public hipc::ShmBaseHeader {
  ShmArchive<vector<list<T>>> numa_lists_;

  explicit ShmHeader(Allocator *alloc, int num_numa) {
    numa_lists_.shm_init(alloc, num_numa);
  }
};

template<typename T>
class numa_list : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))

 public:
  /**====================================
   * Shm Overrides
   * ===================================*/

  /** Default constructor. A default is required. */
  CLASS_NAME() = default;

  /** Default shm constructor */
  void shm_init_main(TYPED_HEADER *header,
                     hipc::Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_make_header(header, alloc_);
  }

  /** Move constructor */
  void shm_strong_move_main(TYPED_HEADER *header,
                          hipc::Allocator *alloc,
                          CLASS_NAME &other) {
    shm_init_main(header, alloc);
  }

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            hipc::Allocator *alloc,
                            const CLASS_NAME &other) {
    shm_init_main(header, alloc);
  }

  /** Destroy the shared-memory data. */
  void shm_destroy_main() {}

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /**====================================
   * Custom methods
   * ===================================*/

  /** Construct an element at the back of the numa_list */
  template<typename... Args>
  void emplace_back(Args&&... args) {
  }

  /** Construct an element at the beginning of the numa_list */
  template<typename... Args>
  void emplace_front(Args&&... args) {
  }

  /** Construct an element at \a pos position in the numa_list */
  template<typename ...Args>
  void emplace(list_iterator<T> pos, Args&&... args) {
  }

  /** Erase element with ID */
  void erase(const T &entry) {
  }

  /** Erase the element at pos */
  void erase(list_iterator<T> pos) {
  }

  /** Erase all elements between first and last */
  void erase(list_iterator<T> first,
             list_iterator<T> last) {
  }

  /** Destroy all elements in the numa_list */
  void clear() {
  }

  /** Get the object at the front of the numa_list */
  Ref<T> front() {
  }

  /** Get the object at the back of the numa_list */
  Ref<T> back() {
  }

  /** Get the number of elements in the numa_list */
  size_t size() const {
    if (!IsNull()) {
      return header_->length_;
    }
    return 0;
  }

  /** Find an element in this numa_list */
  list_iterator<T> find(const T &entry) {
    for (auto iter = begin(); iter != end(); ++iter) {
      hipc::Ref<T> ref = *iter;
      if (*ref == entry) {
        return iter;
      }
    }
    return end();
  }

  /**
   * ITERATORS
   * */

  /** Forward iterator begin */
  list_iterator<T> begin() {
  }

  /** Forward iterator end */
  static list_iterator<T> const end() {
  }

  /** Constant forward iterator begin */
  list_citerator<T> cbegin() const {
  }

  /** Constant forward iterator end */
  static list_citerator<T> const cend() {
  }
};
}  // namespace hermes_shm::ipc

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_NUMA_AWARE_LIST_H_
