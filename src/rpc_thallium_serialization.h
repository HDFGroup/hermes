//
// Created by lukemartinlogan on 12/2/22.
//

#ifndef HERMES_SRC_RPC_THALLIUM_SERIALIZATION_H_
#define HERMES_SRC_RPC_THALLIUM_SERIALIZATION_H_

#include <thallium.hpp>
#include <thallium/serialization/stl/pair.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>

namespace hermes {

/**
 *  Lets Thallium know how to serialize a VBucketID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param vbucket_id The VBucketID to serialize.
 */
template <typename A>
void serialize(A &ar, VBucketID &vbucket_id) {
  ar &vbucket_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a BlobID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param blob_id The BlobID to serialize.
 */
template <typename A>
void serialize(A &ar, BlobID &blob_id) {
  ar &blob_id.as_int;
}

/**
 *  Lets Thallium know how to serialize a TargetID.
 *
 * This function is called implicitly by Thallium.
 *
 * @param ar An archive provided by Thallium.
 * @param target_id The TargetID to serialize.
 */
template <typename A>
void serialize(A &ar, TargetID &target_id) {
  ar &target_id.as_int;
}

}  // namespace hermes

namespace hermes::api {
}  // namespace hermes::api

#endif  // HERMES_SRC_RPC_THALLIUM_SERIALIZATION_H_
