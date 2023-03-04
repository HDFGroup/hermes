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


#ifndef HERMES_DATA_STRUCTURES_SERIALIZATION_THALLIUM_H_
#define HERMES_DATA_STRUCTURES_SERIALIZATION_THALLIUM_H_

#include <thallium.hpp>
#include <hermes_shm/data_structures/string.h>
#include <hermes_shm/data_structures/thread_unsafe/vector.h>
#include <hermes_shm/data_structures/thread_unsafe/list.h>

namespace thallium {

namespace hipc = hermes_shm::ipc;

/**
 *  Lets Thallium know how to serialize an hipc::allocator_id.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param vec The vector to serialize.
 */
template <typename A>
void serialize(A &ar, hipc::allocator_id_t &alloc_id) {
  ar &alloc_id.int_;
}

/**
 *  Lets Thallium know how to serialize an hipc::vector.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param vec The vector to serialize.
 */
template <typename A, typename T>
void save(A &ar, hipc::vector<T> &vec) {
  ar << vec.GetAllocatorId();
  ar << vec.size();
  for (auto iter = vec.cbegin(); iter != vec.cend(); ++iter) {
    ar << *(*iter);
  }
}

/**
 *  Lets Thallium know how to deserialize an hipc::vector.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The vector to serialize.
 */
template <typename A, typename T>
void load(A &ar, hipc::vector<T> &vec) {
  size_t size;
  hipc::allocator_id_t alloc_id;
  ar >> alloc_id;
  ar >> size;
  auto alloc = HERMES_MEMORY_MANAGER->GetAllocator(alloc_id);
  vec.shm_init(alloc);
  vec.resize(size);
  for (auto iter = vec.begin(); iter != vec.end(); ++iter) {
    vec.emplace_back(std::move(**iter));
  }
}

/**
 *  Lets Thallium know how to serialize an hipc::string.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param text The string to serialize
 */
template <typename A>
void save(A &ar, hipc::string &text) {
  ar << text.GetAllocator()->GetId();
  ar << text.size();
  ar.write(text.data_mutable(), text.size());
}

/**
 *  Lets Thallium know how to deserialize an hipc::string.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The string to deserialize.
 */
template <typename A>
void load(A &ar, hipc::string &text) {
  hipc::allocator_id_t alloc_id;
  size_t size;
  ar >> alloc_id;
  ar >> size;
  auto alloc = HERMES_MEMORY_MANAGER->GetAllocator(alloc_id);
  text.shm_init(alloc, size);
  ar.read(text.data_mutable(), size);
}

}  // namespace thallium

#endif  // HERMES_DATA_STRUCTURES_SERIALIZATION_THALLIUM_H_
