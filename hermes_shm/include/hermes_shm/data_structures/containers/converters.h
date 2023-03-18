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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_CONVERTERS_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_CONVERTERS_H_

#include "hermes_shm/data_structures/data_structure.h"
#include <vector>
#include <list>

namespace hermes_shm {

/** Convert an iterable object into a vector */
template<typename T, typename SharedT>
std::vector<T> to_stl_vector(const SharedT &other) {
  std::vector<T> vec;
  vec.reserve(other.size());
  auto end = other.cend();
  for (auto iter = other.cbegin(); iter != end; ++iter) {
    hipc::Ref<T> obj = (*iter);
    vec.emplace_back(*obj);
  }
  return vec;
}

/** Convert an iterable object into a list */
template<typename T, typename SharedT>
std::list<T> to_stl_list(const SharedT &other) {
  std::list<T> vec;
  auto end = other.cend();
  for (auto iter = other.cbegin(); iter != end; ++iter) {
    hipc::Ref<T> obj = (*iter);
    vec.emplace_back(*obj);
  }
  return vec;
}

/** Convert a string to an hshm::charbuf */
template<typename StringT>
hshm::charbuf to_charbuf(StringT &other) {
  hshm::charbuf text(other.size());
  memcpy(text.data(), other.data(), other.size());
  return text;
}

}  // namespace hermes_shm

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_DATA_STRUCTURES_IPC_CONVERTERS_H_
