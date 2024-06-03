//
// Created by lukemartinlogan on 9/30/23.
//

#ifndef HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_
#define HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_

#include "../data_stager.h"
#include "abstract_stager.h"
#include "binary_stager.h"
#ifdef HERMES_ENABLE_ADIOS
#include "adios2.h"
#include "adios2_stager.h"
#endif

namespace hermes::data_stager {

class StagerFactory {
 public:
  static std::unique_ptr<AbstractStager> Get(const std::string &path,
                                             const std::string &params) {
    std::string protocol;
    hrun::LocalDeserialize srl(params);
    srl >> protocol;

    std::unique_ptr<AbstractStager> stager;
    if (protocol == "file") {
      stager = std::make_unique<BinaryFileStager>();
    } else if (protocol == "parquet") {
    } else if (protocol == "hdf5") {
    } else if (protocol == "adios2") {
#ifdef HERMES_ENABLE_ADIOS
      stager = std::make_unique<Adios2Stager>();
#endif
    } else {
      throw std::runtime_error("Unknown stager type");
    }
    stager->path_ = path;
    stager->params_ = params;
    return stager;
  }
};

}  // namespace hermes

#endif  // HERMES_TASKS_DATA_STAGER_SRC_STAGER_FACTORY_H_
