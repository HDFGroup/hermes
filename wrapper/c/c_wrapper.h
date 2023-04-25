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

#ifndef HERMES_WRAPPER_C_C_WRAPPER_H_
#define HERMES_WRAPPER_C_C_WRAPPER_H_

#include "hermes.h"

extern "C" {

hapi::Hermes* HERMES_Get();
void HERMES_Create(hermes::HermesType mode,
                   std::string server_config_path,
                   std::string client_config_path);
hapi::Bucket HERMES_Bucket_get(const std::string &name);
void HERMES_Bucket_destroy(hapi::Bucket &bucket);
hermes::BlobId HERMES_Bucket_put_blob(hapi::Bucket &bucket,
                                      const char *blob_name,
                                      const char *blob,
                                      size_t blob_size);
hermes::BlobId HERMES_Bucket_get_blob_id(hapi::Bucket &bucket,
                                         const char *blob_name);
void HERMES_Bucket_get_blob(hapi::Bucket &bucket,
                            hermes::BlobId &blob_id,
                            char* &blob,
                            size_t &blob_size);
void HERMES_Bucket_destroy_blob(hapi::Bucket &bucket,
                                hermes::BlobId &blob_id);

}  // extern C

#endif  // HERMES_WRAPPER_C_C_WRAPPER_H_
