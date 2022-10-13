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

#ifndef HERMES_DATA_STAGER_STAGE_FACTORY_H_
#define HERMES_DATA_STAGER_STAGE_FACTORY_H_

#include "data_stager.h"
#include "stagers/unix_stager.h"

namespace hermes {

class DataStagerFactory {
 public:
  static std::unique_ptr<DataStager> Get(const std::string& url) {
    return Get(DataStagerTypeConv::from_url(url));
  }

  static std::unique_ptr<DataStager> Get(DataStagerType stager) {
    switch (stager) {
      case DataStagerType::kUnix: {
        return std::make_unique<UnixStager>();
      }
      default: {
        return nullptr;
      }
    }
  }
};

}
#endif  // HERMES_DATA_STAGER_STAGE_FACTORY_H_
