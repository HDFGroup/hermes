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

#ifndef HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_VECTOR_H_
#define HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_VECTOR_H_

#include "list.h"

template<typename T, typename Container>
class VectorTestSuite : public ListTestSuite<T, Container> {
 public:
  using ListTestSuite<T, Container>::obj_;

 public:
  /// Constructor
  VectorTestSuite(Container &obj, Allocator *alloc)
  : ListTestSuite<T, Container>(obj, alloc) {}

  /// Test vector index operator
  void IndexTest() {
    for (int i = 0; i < obj_.size(); ++i) {
      CREATE_SET_VAR_TO_INT_OR_STRING(T, var, i);
      REQUIRE(*obj_[i] == var);
    }
  }
};

#endif //HERMES_SHM_TEST_UNIT_DATA_STRUCTURES_CONTAINERS_VECTOR_H_
