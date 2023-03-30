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
#include "hermes_shm/data_structures/ipc/string.h"
#include <hermes_shm/data_structures/data_structure.h>
#include <hermes_shm/data_structures/ipc/vector.h>
#include <hermes_shm/data_structures/ipc/list.h>
#include "hermes_shm/data_structures/containers/charbuf.h"
#include "hermes_shm/data_structures/ipc/slist.h"

namespace thallium {

/**====================================
 * Identifier Serialization
 * ===================================*/

/** Lets Thallium know how to serialize an hipc::allocator_id */
template <typename A>
void serialize(A &ar, hipc::allocator_id_t &alloc_id) {
  ar &alloc_id.int_;
}

/**====================================
 * Vector Serialization
 * ===================================*/

/** Lets Thallium know how to serialize a generic vector */
template <typename A, typename ContainerT, typename T>
void save_vec(A &ar, ContainerT &obj) {
  ar << obj.size();
  for (auto iter = obj.cbegin(); iter != obj.cend(); ++iter) {
    ar << *(*iter);
  }
}

/** Lets Thallium know how to deserialize a generic vector */
template <typename A, typename ContainerT, typename T>
void load_vec(A &ar, ContainerT &obj) {
  size_t size;
  hipc::allocator_id_t alloc_id;
  ar >> size;
  obj.resize(size);
  for (size_t i = 0; i < size; ++i) {
    ar >> *(obj[i]);
  }
}

/** Lets Thallium know how to serialize an hipc::vector. */
template <typename A, typename T>
void save(A &ar, hipc::vector<T> &vec) {
  save_vec<A, hipc::vector<T>, T>(ar, vec);
}
template <typename A, typename T>
void save(A &ar, hipc::uptr<hipc::vector<T>> &vec) {
  save_vec<A, hipc::vector<T>, T>(ar, *vec);
}

/** Lets Thallium know how to deserialize an hipc::vector. */
template <typename A, typename T>
void load(A &ar, hipc::vector<T> &vec) {
  load_vec<A, hipc::vector<T>, T>(ar, vec);
}
template <typename A, typename T>
void load(A &ar, hipc::uptr<hipc::vector<T>> &vec) {
  vec = hipc::make_uptr<hipc::vector<T>>();
  load_vec<A, hipc::vector<T>, T>(ar, *vec);
}

/**====================================
 * List Serialization
 * ===================================*/

/** Lets Thallium know how to serialize a generic list */
template <typename A, typename ContainerT, typename T>
void save_list(A &ar, ContainerT &obj) {
  ar << obj.size();
  for (auto iter = obj.cbegin(); iter != obj.cend(); ++iter) {
    ar << *(*iter);
  }
}

/** Lets Thallium know how to deserialize a generic list */
template <typename A, typename ContainerT, typename T>
void load_list(A &ar, ContainerT &obj) {
  size_t size;
  ar >> size;
  for (int i = 0; i < size; ++i) {
    obj->emplace_back();
    auto last = obj->back();
    ar >> (*last);
  }
}

/** Lets Thallium know how to serialize an hipc::slist. */
template <typename A, typename T>
void save(A &ar, hipc::slist<T> &lp) {
  save_list<A, hipc::slist<T>, T>(ar, lp);
}
template <typename A, typename T>
void save(A &ar, hipc::uptr<hipc::slist<T>> &lp) {
  save_list<A, hipc::slist<T>, T>(ar, *lp);
}

/** Lets Thallium know how to deserialize an hipc::slist. */
template <typename A, typename T>
void load(A &ar, hipc::slist<T> &lp) {
  load_list<A, hipc::slist<T>, T>(ar, lp);
}
template <typename A, typename T>
void load(A &ar, hipc::uptr<hipc::slist<T>> &lp) {
  lp = hipc::make_uptr<hipc::slist<T>>();
  load_list<A, hipc::slist<T>, T>(ar, *lp);
}

/** Lets Thallium know how to serialize an hipc::list. */
template <typename A, typename T>
void save(A &ar, const hipc::list<T> &lp) {
  save_list<A, hipc::list<T>, T>(ar, *lp);
}
template <typename A, typename T>
void save(A &ar, hipc::uptr<hipc::list<T>> &lp) {
  save_list<A, hipc::list<T>, T>(ar, *lp);
}

/** Lets Thallium know how to deserialize an hipc::list. */
template <typename A, typename T>
void load(A &ar, hipc::list<T> &lp) {
  load_list<A, hipc::list<T>, T>(ar, lp);
}
template <typename A, typename T>
void load(A &ar, hipc::uptr<hipc::list<T>> &lp) {
  lp = hipc::make_uptr<hipc::list<T>>();
  load_list<A, hipc::list<T>, T>(ar, *lp);
}

/**====================================
 * String Serialization
 * ===================================*/

/** Lets Thallium know how to serialize a generic string. */
template <typename A, typename StringT>
void save_string(A &ar, StringT &text) {
  ar << text.size();
  ar.write(text.data(), text.size());
}
/** Lets Thallium know how to serialize a generic string. */
template <typename A, typename StringT>
void load_string(A &ar, StringT &text) {
  size_t size;
  ar >> size;
  text.resize(size);
  ar.read(text.data(), text.size());
}

/** Lets Thallium know how to serialize an hipc::string. */
template <typename A>
void save(A &ar, hipc::string &text) {
  save_string<A, hipc::string>(ar, text);
}
template <typename A>
void save(A &ar, hipc::uptr<hipc::string> &text) {
  save_string<A, hipc::string>(ar, *text);
}

/** Lets Thallium know how to deserialize an hipc::string. */
template <typename A>
void load(A &ar, hipc::string &text) {
  load_string<A, hipc::string>(ar, text);
}
template <typename A>
void load(A &ar, hipc::uptr<hipc::string> &text) {
  text = hipc::make_uptr<hipc::string>();
  load_string<A, hipc::string>(ar, *text);
}

/** Lets Thallium know how to serialize an hshm::charbuf. */
template <typename A>
void save(A &ar, hshm::charbuf &text) {
  save_string<A, hshm::charbuf>(ar, text);
}

/** Lets Thallium know how to deserialize an hshm::charbuf. */
template <typename A>
void load(A &ar, hshm::charbuf &text) {
  load_string<A, hshm::charbuf>(ar, text);
}

/**====================================
 * Bitfield Serialization
 * ===================================*/

/** Lets Thallium know how to serialize a bitfield. */
template <typename A>
void save(A &ar, hshm::bitfield32_t &field) {
  uint32_t bits = field.bits_;
  ar << bits;
}

/** Lets Thallium know how to deserialize a bitfield. */
template <typename A>
void load(A &ar, hshm::bitfield32_t &field) {
  uint32_t bits;
  ar >> bits;
  field.bits_ = bits;
}

}  // namespace thallium

#endif  // HERMES_DATA_STRUCTURES_SERIALIZATION_THALLIUM_H_
