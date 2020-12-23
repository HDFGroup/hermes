//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_BALANCED_MAPPER_H
#define HERMES_BALANCED_MAPPER_H

#include <vector>

#include "abstract_mapper.h"

namespace hermes::adapter::stdio {
class BalancedMapper : public AbstractMapper {
 public:
  MapperReturnType map(const FileStruct& file_op) override;
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_BALANCED_MAPPER_H
