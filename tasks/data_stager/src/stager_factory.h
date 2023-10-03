//
// Created by lukemartinlogan on 9/30/23.
//

#ifndef HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_
#define HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_

#include "data_stager/data_stager.h"
#include "abstract_stager.h"
#include "binary_stager.h"

namespace hermes::data_stager {

class StagerFactory {
 public:
  static std::unique_ptr<AbstractStager> Get(const std::string &url) {
    std::unique_ptr<AbstractStager> stager;
    if (url.find("file://") == 0) {
      stager = std::make_unique<BinaryFileStager>();
    } else if (url.find("parquet://")) {
    } else if (url.find("hdf5://")) {
    } else {
      throw std::runtime_error("Unknown stager type");
    }
    stager->url_ = url;
    return stager;
  }
};

}  // namespace hermes

#endif  // HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_
