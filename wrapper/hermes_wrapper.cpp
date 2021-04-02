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

#include "hermes_wrapper.h"

#include "hermes.h"
#include "bucket.h"

extern "C" {

std::shared_ptr<hermes::api::Hermes> hermes_ptr;
hermes::api::Context ctx;

int HermesInitHermes(char *hermes_config) {
  int result = 0;

  LOG(INFO) << "Wrapper: Initialize Hermes\n";

  hermes_ptr = hermes::InitHermesDaemon(hermes_config);

  return result;
}

BucketClass *HermesBucketCreate(const char *name) {
  LOG(INFO) << "Wrapper: Create Bucket " << name << '\n';

  try {
    return reinterpret_cast<BucketClass *>(new hermes::api::Bucket(
                                           std::string(name), hermes_ptr, ctx));
  }
  catch (const std::runtime_error& e) {
    LOG(ERROR) << "Blob runtime error\n";
    return NULL;
  }
  catch (const std::length_error& e) {
    LOG(ERROR) << "Blob length error\n";
    return NULL;
  }
}

void HermesBucketClose(BucketClass *bkt) {
  LOG(INFO) << "Wrapper: Close Bucket " <<
               reinterpret_cast<hermes::api::Bucket *>(bkt)->GetName() << '\n';

  reinterpret_cast<hermes::api::Bucket *>(bkt)->Close(ctx);
}

void HermesBucketDestroy(BucketClass *bkt_ptr) {
  LOG(INFO) << "Destroy Bucket\n";

  delete reinterpret_cast<hermes::api::Bucket *>(bkt_ptr);
}

}  // extern "C"
