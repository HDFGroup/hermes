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

#ifndef HERMES_MPIIO_COMMON_CONSTANTS_H
#define HERMES_MPIIO_COMMON_CONSTANTS_H

#include "mpiio/enumerations.h"

/**
 * Constants file for MPIIO adapter.
 */
using hermes::adapter::mpiio::MapperType;

/**
 * Which mapper to be used by MPIIO adapter.
 */
const MapperType kMapperType = MapperType::BALANCED;

/**
 * String delimiter
 */
const char kStringDelimiter = '#';

/**
 * Number of threads for thread pool of async I/O APIs
 */
const int kNumThreads = 1;
#endif  // HERMES_MPIIO_COMMON_CONSTANTS_H
