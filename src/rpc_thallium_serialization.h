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

namespace hermes {

/**
 *  Lets Thallium know how to serialize a BucketId.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param bucket_id The BucketId to serialize.
 */
template <typename A>
void serialize(A &ar, BucketId &bucket_id) {
  ar &bucket_id.unique_;
  ar &bucket_id.node_id_;
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

/** Let's thallium know how to serialize an MdLockType */
template <typename A>
void serialize(A &ar, MdLockType lock) {
  ar &static_cast<int>(lock);
}

}  // namespace hermes

namespace hermes::adapter {

/** Let's thallium know how to serialize an AdapterMode */
template <typename A>
void serialize(A &ar, AdapterMode mode) {
  ar &static_cast<int>(mode);
}

/** Let's thallium know how to serialize an AdapterType */
template <typename A>
void serialize(A &ar, AdapterType mode) {
  ar &static_cast<int>(mode);
}

/** Let's thallium know how to serialize an IoClientContext */
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
/** Let's thallium know how to serialize a Status */
template <typename A>
void serialize(A &ar, Status status) {
  ar &status.code_;
  ar &status.msg_;
}
}  // namespace hermes::api

#endif  // HERMES_SRC_RPC_THALLIUM_SERIALIZATION_H_
