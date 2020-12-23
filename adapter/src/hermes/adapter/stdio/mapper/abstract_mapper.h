//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_ABSTRACT_ADAPTER_H
#define HERMES_ABSTRACT_ADAPTER_H

#include <hermes/adapter/stdio/common/datastructures.h>

typedef std::vector<std::pair<hermes::adapter::stdio::FileStruct,
                              hermes::adapter::stdio::HermesStruct>>
    MapperReturnType;

namespace hermes::adapter::stdio {
class AbstractMapper {
 public:
  virtual MapperReturnType map(const FileStruct& file_op) = 0;
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_ABSTRACT_ADAPTER_H
