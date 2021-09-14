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
#include "vbucket.h"

extern "C" {

std::shared_ptr<hermes::api::Hermes> hermes_ptr;

int HermesInitHermes(char *hermes_config) {
  int result = 0;

  LOG(INFO) << "Hermes Wrapper: Initializing Hermes\n";

  hermes_ptr = hermes::InitHermesDaemon(hermes_config);

  return result;
}

void HermesFinalize() {
  hermes_ptr->Finalize(true);
}

void HermesVBucketLink(VBucketClass *vbkt, char *blob_name) {
  hermes::api::VBucket *vbucket = (hermes::api::VBucket *)vbkt;

  LOG(INFO) << "Hermes Wrapper: Linking Blob " << blob_name << "to VBucket "
            << vbucket->GetName() << '\n';

  hermes::api::Status status =
    vbucket->Link(std::string(blob_name), vbucket->GetName());

  if (status.Failed())
    LOG(ERROR) << "Hermes Wrapper: HermesVBucketLink failed\n";
}

bool HermesVBucketIsValid(VBucketClass *vbkt) {
  hermes::api::VBucket *vbucket = (hermes::api::VBucket *)vbkt;

  LOG(INFO) << "Hermes Wrapper: Checking if VBucket is valid\n";

  return vbucket->IsValid();
}

BucketClass *HermesBucketCreate(const char *name) {
  LOG(INFO) << "Hermes Wrapper: Creating Bucket " << name << '\n';

  try {
    hermes::api::Bucket *new_bucket =
      new hermes::api::Bucket(std::string(name), hermes_ptr);

    return (BucketClass *)new_bucket;
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
  hermes::api::Bucket *my_bkt = (hermes::api::Bucket *)bkt;

  LOG(INFO) << "Hermes Wrapper: Closing Bucket " << my_bkt->GetName() << '\n';

  my_bkt->Release();
  delete my_bkt;
}

void HermesBucketDestroy(BucketClass *bkt) {
  hermes::api::Bucket *my_bkt = (hermes::api::Bucket *)bkt;

  LOG(INFO) << "Hermes Wrapper: Destroying Bucket\n";

  my_bkt->Destroy();
}

bool HermesBucketContainsBlob(BucketClass *bkt, char *name) {
  hermes::api::Bucket *bucket = (hermes::api::Bucket *)bkt;

  LOG(INFO) << "Hermes Wrapper: Checking if Bucket "
            << bucket->GetName()
            << " contains Blob " << name << '\n';

  return bucket->ContainsBlob(name);
}

void HermesBucketPut(BucketClass *bkt, char *name, unsigned char *put_data,
                    size_t size) {
  hermes::api::Bucket *bucket = (hermes::api::Bucket *)bkt;

  LOG(INFO) << "Hermes Wrapper: Putting Blob " << name << " to bucket " <<
               bucket->GetName() << '\n';

  hermes::api::Status status = bucket->Put(name, put_data, size);

  if (status.Failed())
    LOG(ERROR) << "Hermes Wrapper: HermesBucketPut failed\n";
}

void HermesBucketGet(BucketClass *bkt, char *blob_name, size_t page_size,
                     unsigned char *buf) {
  hermes::api::Bucket *bucket = (hermes::api::Bucket *)bkt;
  const hermes::api::Context ctx;

  LOG(INFO) << "Hermes Wrapper: Getting blob " << blob_name << " from Bucket "
            << bucket->GetName();

  size_t blob_size = bucket->Get(blob_name, buf, page_size, ctx);
  if (blob_size != page_size)
    LOG(ERROR) << "Blob size error: expected to get " << page_size
               << ", but only get " << blob_size << '\n';
}

}  // extern "C"
