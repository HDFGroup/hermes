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

#ifndef HERMES_SRC_RPC_THALLIUM_SERIALIZATION_H_
#define HERMES_SRC_RPC_THALLIUM_SERIALIZATION_H_

#include <thallium.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/list.hpp>

#include "hermes_types.h"
#include "statuses.h"
#include "metadata_types.h"
#include "data_structures.h"
#include "hermes_shm/data_structures/serialization/thallium.h"

/** Lets thallium know how to serialize an enum */
#define SERIALIZE_ENUM(T)\
  template <typename A>\
  void save(A &ar, T &mode) {\
    int cast = static_cast<int>(mode);\
    ar << cast;\
  }\
  template <typename A>\
  void load(A &ar, T &mode) {\
    int cast;\
    ar >> cast;\
    mode = static_cast<T>(cast);\
  }

namespace hermes {

/**
 *  Lets Thallium know how to serialize a TagId.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param bucket_id The TagId to serialize.
 */
template <typename A>
void serialize(A &ar, TagId &tag_id) {
  ar &tag_id.unique_;
  ar &tag_id.node_id_;
}

/**
 *  Lets Thallium know how to serialize a BlobId.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param blob_id The BlobId to serialize.
 */
template <typename A>
void serialize(A &ar, BlobId &blob_id) {
  ar &blob_id.unique_;
  ar &blob_id.node_id_;
}

/**
 *  Lets Thallium know how to serialize a TraitId.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param blob_id The BlobId to serialize.
 */
template <typename A>
void serialize(A &ar, TraitId &trait_id) {
  ar &trait_id.unique_;
  ar &trait_id.node_id_;
}

/**
 *  Lets Thallium know how to serialize a TargetId.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The TargetId to serialize.
 */
template <typename A>
void serialize(A &ar, TargetId &target_id) {
  ar &target_id.as_int_;
}

/**
 *  Lets Thallium know how to serialize a TargetId.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The TargetId to serialize.
 */
template <typename A>
void serialize(A &ar, BufferInfo &info) {
  ar &info.tid_;
  ar &info.t_off_;
  ar &info.t_size_;
  ar &info.blob_off_;
  ar &info.blob_size_;
}

/** Lets thallium know how to serialize an MdLockType */
SERIALIZE_ENUM(MdLockType)

/** Lets thallium know how to serialize a SubPlacement */
template <typename A>
void serialize(A &ar, SubPlacement &plcmnt) {
  ar &plcmnt.size_;
  ar &plcmnt.tid_;
}

/** Lets thallium know how to serialize a PlacementSchema */
template <typename A>
void serialize(A &ar, PlacementSchema &schema) {
  ar &schema.plcmnts_;
}

}  // namespace hermes

namespace hermes::adapter {

/** Lets thallium know how to serialize an AdapterMode */
SERIALIZE_ENUM(AdapterMode)

/** Lets thallium know how to serialize an AdapterType */
SERIALIZE_ENUM(AdapterType)

/** Lets thallium know how to serialize an IoClientContext */
template <typename A>
void serialize(A &ar, IoClientContext &opts) {
  ar &opts.type_;
  ar &opts.adapter_mode_;
  ar &opts.dpe_;
  ar &opts.flags_;
  ar &opts.mpi_type_;
  ar &opts.mpi_count_;
  ar &opts.backend_off_;
  ar &opts.backend_size_;
}

}  // namespace hermes::adapter

namespace hermes::api {

/** Lets thallium know how to serialize a Status */
template <typename A>
void save(A &ar, Status &status) {
  ar << status.code_;
}

/** Lets thallium know how to deserialize a Status */
template <typename A>
void load(A &ar, Status &status) {
  int code;
  ar >> code;
  status = Status(code);
}

/** Lets thallium know how to serialize a PlacementPolicy */
SERIALIZE_ENUM(PlacementPolicy)

}  // namespace hermes::api

#endif  // HERMES_SRC_RPC_THALLIUM_SERIALIZATION_H_
