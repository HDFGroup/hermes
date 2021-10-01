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

#ifndef HERMES_STDIO_COMMON_CONSTANTS_H
#define HERMES_STDIO_COMMON_CONSTANTS_H

/**
 * Standard header
 */
#include <cstdlib>

/**
 * Dependent library header
 */
#include "glog/logging.h"

/**
 * Internal header
 */
#include <hermes/adapter/stdio/common/enumerations.h>

/**
 * Constants file for STDIO adapter.
 */
using hermes::adapter::stdio::MapperType;

/**
 * Which mapper to be used by STDIO adapter.
 */
const MapperType kMapperType = MapperType::BALANCED;

/**
 * Define kPageSize for balanced mapping.
 */
const size_t kPageSize = []() {
  const char *kPageSizeVar = "HERMES_PAGE_SIZE";
  const size_t kDefaultPageSize = 1 * 1024 * 1024;

  size_t result = kDefaultPageSize;
  char *page_size = getenv(kPageSizeVar);

  if (page_size) {
    result = (size_t)std::strtoull(page_size, NULL, 0);
    if (result == 0) {
      LOG(FATAL) << "Invalid value of " << kPageSizeVar << ": " << page_size;
    }
  }

  LOG(INFO) << "Stdio adapter page size: " << result << "\n";

  return result;
}();

/**
 * String delimiter
 */
const char kStringDelimiter = '#';

#endif  // HERMES_STDIO_COMMON_CONSTANTS_H
