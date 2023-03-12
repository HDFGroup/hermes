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
#include <hermes_shm/data_structures/data_structure.h>
#include <hermes_shm/data_structures/thread_unsafe/vector.h>
#include <hermes_shm/data_structures/thread_unsafe/list.h>
#include <hermes_shm/types/charbuf.h>
#include "hermes_shm/data_structures/thread_unsafe/slist.h"

namespace thallium {

namespace hipc = hermes_shm::ipc;
namespace hshm = hermes_shm;

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
 *  Lets Thallium know how to serialize a linear container type
 *  (e.g., vector).
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param vec The vector to serialize.
 */
template <typename A, typename ContainerT, typename T>
void save_vec(A &ar, ContainerT &obj) {
  ar << obj.GetAllocatorId();
  ar << obj.size();
  for (auto iter = obj.cbegin(); iter != obj.cend(); ++iter) {
    ar << *(*iter);
  }
}

/**
 *  Lets Thallium know how to deserialize a linear container type
 *  (e.g., vector).
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The vector to serialize.
 */
template <typename A, typename ContainerT, typename T>
void load_vec(A &ar, ContainerT &obj) {
  size_t size;
  hipc::allocator_id_t alloc_id;
  ar >> alloc_id;
  ar >> size;
  auto alloc = HERMES_MEMORY_MANAGER->GetAllocator(alloc_id);
  obj.shm_init(alloc);
  obj.resize(size);
  for (int i = 0; i < size; ++i) {
    ar >> *(obj[i]);
  }
}

/**
 *  Lets Thallium know how to serialize a linear container type
 *  (e.g., vector).
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param vec The vector to serialize.
 */
template <typename A, typename ContainerT, typename T>
void save_list(A &ar, ContainerT &obj) {
  ar << obj.GetAllocatorId();
  ar << obj.size();
  for (auto iter = obj.cbegin(); iter != obj.cend(); ++iter) {
    ar << *(*iter);
  }
}

/**
 *  Lets Thallium know how to deserialize a linear container type
 *  (e.g., vector).
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The vector to serialize.
 */
template <typename A, typename ContainerT, typename T>
void load_list(A &ar, ContainerT &obj) {
  size_t size;
  hipc::allocator_id_t alloc_id;
  ar >> alloc_id;
  ar >> size;
  auto alloc = HERMES_MEMORY_MANAGER->GetAllocator(alloc_id);
  obj.shm_init(alloc);
  for (int i = 0; i < size; ++i) {
    T elmt;
    ar >> elmt;
    obj.emplace_back(std::move(elmt));
  }
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
  save_vec<A, hipc::vector<T>, T>(ar, vec);
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
  load_vec<A, hipc::vector<T>, T>(ar, vec);
}

/**
 *  Lets Thallium know how to serialize an hipc::slist.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param vec The vector to serialize.
 */
template <typename A, typename T>
void save(A &ar, hipc::slist<T> &lp) {
  save_list<A, hipc::slist<T>, T>(ar, lp);
}

/**
 *  Lets Thallium know how to deserialize an hipc::slist.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The vector to serialize.
 */
template <typename A, typename T>
void load(A &ar, hipc::slist<T> &lp) {
  load_list<A, hipc::slist<T>, T>(ar, lp);
}

/**
 *  Lets Thallium know how to serialize an hipc::list.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param vec The vector to serialize.
 */
template <typename A, typename T>
void save(A &ar, hipc::list<T> &lp) {
  save_list<A, hipc::list<T>, T>(ar, lp);
}

/**
 *  Lets Thallium know how to deserialize an hipc::list.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The vector to serialize.
 */
template <typename A, typename T>
void load(A &ar, hipc::list<T> &lp) {
  load_list<A, hipc::list<T>, T>(ar, lp);
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
  ar << text.size();
  if (!text.size()) { return; }
  ar << text.GetAllocator()->GetId();
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
  ar >> size;
  if (!size) { return; }
  ar >> alloc_id;
  auto alloc = HERMES_MEMORY_MANAGER->GetAllocator(alloc_id);
  text.shm_init(alloc, size);
  ar.read(text.data_mutable(), size);
}

/**
 *  Lets Thallium know how to serialize an hshm::charbuf.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param text The string to serialize
 */
template <typename A>
void save(A &ar, hshm::charbuf &text) {
  ar << text.size();
  if (!text.size()) { return; }
  ar << text.GetAllocator()->GetId();
  ar.write(text.data(), text.size());
}

/**
 *  Lets Thallium know how to deserialize an hshm::charbuf.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The string to deserialize.
 */
template <typename A>
void load(A &ar, hshm::charbuf &text) {
  hipc::allocator_id_t alloc_id;
  size_t size;
  ar >> size;
  if (!size) { return; }
  ar >> alloc_id;
  auto alloc = HERMES_MEMORY_MANAGER->GetAllocator(alloc_id);
  text = hshm::charbuf(alloc, size);
  ar.read(text.data(), size);
}

/**
 *  Lets Thallium know how to serialize a bitfield.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param field the bitfield to serialize
 */
template <typename A>
void save(A &ar, hshm::bitfield32_t &field) {
  uint32_t bits = field.bits_;
  ar << bits;
}

/**
 *  Lets Thallium know how to deserialize a bitfield.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param field the bitfield to serialize
 */
template <typename A>
void load(A &ar, hshm::bitfield32_t &field) {
  uint32_t bits;
  ar >> bits;
  field.bits_ = bits;
}

/**
 * Lets Thallium know how to serialize a ShmRef object
 * */
template<typename A, typename T>
void serialize(A &ar, hipc::ShmRef<T> &ref) {
  ar &(*ref);
}

}  // namespace thallium

#endif  // HERMES_DATA_STRUCTURES_SERIALIZATION_THALLIUM_H_
