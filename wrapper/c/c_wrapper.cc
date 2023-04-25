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

#include "hermes.h"
#include "c_wrapper.h"

extern "C" {

/** Get the Hermes instance */
hapi::Hermes* HERMES_Get() {
  return HERMES;
}

/** Initialize a Hermes instance */
void HERMES_Create(hermes::HermesType mode,
                   std::string server_config_path,
                   std::string client_config_path) {
  HERMES->Create(mode, server_config_path, client_config_path);
}

/** Bucket create */
hapi::Bucket HERMES_Bucket_get(const std::string &name) {
  hapi::Context ctx;
  return HERMES->GetBucket(name, ctx, 0);
}

/** Bucket delete */
void HERMES_Bucket_destroy(hapi::Bucket &bucket) {
  bucket.Destroy();
}

/** Blob put */
hermes::BlobId HERMES_Bucket_put_blob(hapi::Bucket &bucket,
                                      const char *blob_name,
                                      const char *blob,
                                      size_t blob_size) {
  hermes::Blob blob_wrap(blob, blob_size);
  hermes::BlobId blob_id;
  bucket.Put(blob_name, blob_wrap, blob_id, bucket.GetContext());
  return blob_id;
}

/** Get blob ID */
hermes::BlobId HERMES_Bucket_get_blob_id(hapi::Bucket &bucket,
                                         const char *blob_name) {
  hermes::BlobId blob_id;
  bucket.GetBlobId(blob_name, blob_id);
  return blob_id;
}

/** Blob get */
void HERMES_Bucket_get_blob(hapi::Bucket &bucket,
                            hermes::BlobId &blob_id,
                            char* &blob,
                            size_t &blob_size) {
  hermes::Blob blob_h;
  bucket.Get(blob_id, blob_h, bucket.GetContext());
  blob = blob_h.data();
  blob_size = blob_h.size();
  std::move(blob_h);  // Don't allow blob to be destroyed
}

/** Blob delete */
void HERMES_Bucket_destroy_blob(hapi::Bucket &bucket,
                                hermes::BlobId &blob_id) {
  bucket.DestroyBlob(blob_id, bucket.GetContext());
}

}  // extern C