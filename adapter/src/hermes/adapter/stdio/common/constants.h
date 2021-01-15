//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_STDIO_COMMON_CONSTANTS_H
#define HERMES_STDIO_COMMON_CONSTANTS_H

#include <hermes/adapter/stdio/common/enumerations.h>
#include <hermes/adapter/constants.h>
using hermes::adapter::stdio::MapperType;

const MapperType MAPPER_TYPE = MapperType::BALANCED;
const size_t PAGE_SIZE = 1024 * 1024;

#endif  // HERMES_STDIO_COMMON_CONSTANTS_H
