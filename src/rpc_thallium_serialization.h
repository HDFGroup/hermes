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
#include "metadata_types.h"
#include "data_structures.h"

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

}  // namespace hermes

namespace hermes::api {
}  // namespace hermes::api

#endif  // HERMES_SRC_RPC_THALLIUM_SERIALIZATION_H_
