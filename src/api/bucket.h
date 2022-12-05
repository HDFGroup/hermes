//
// Created by lukemartinlogan on 12/4/22.
//

#ifndef HERMES_SRC_API_BUCKET_H_
#define HERMES_SRC_API_BUCKET_H_

#include "hermes_types.h"
#include "hermes_status.h"
#include "hermes.h"

namespace hermes::api {

class Bucket {
 private:
  MetadataManager *mdm_;

 public:
  Bucket(std::string name,
         Context &ctx);

  /* std::string GetName() const;

  u64 GetId() const; */

 public:
  RPC_AUTOGEN_START
  RPC_AUTOGEN_END
};

}

#endif  // HERMES_SRC_API_BUCKET_H_
