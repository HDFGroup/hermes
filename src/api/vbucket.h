//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_API_VBUCKET_H_
#define HERMES_SRC_API_VBUCKET_H_

#include "hermes_types.h"
#include "hermes_status.h"
#include "hermes.h"

namespace hermes::api {

class VBucket {
 public:
  VBucket(std::string name, Context &ctx);

  /*void Link(std::string bucket_name,
            std::string blob_name, Context ctx = Context());

  void Link(std::shared_ptr<Bucket> &bucket,
            std::string blob_name, Context ctx = Context());

  void Unlink(std::string bucket_name,
              std::string blob_name, Context ctx = Context());

  void Unlink(std::shared_ptr<Bucket> &bucket,
              std::string blob_name, Context ctx = Context());

  Status Destroy();

  bool ContainsBlob(std::string bucket_name, std::string blob_name);

  size_t Get();

  template<typename T, typename ...Args>
  void Attach(Args ...args) {
  }*/
};

}  // namespace hermes::api

#endif  // HERMES_SRC_API_VBUCKET_H_
