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

#ifndef HERMES_STDIO_COMMON_ENUMERATIONS_H
#define HERMES_STDIO_COMMON_ENUMERATIONS_H
/**
 * Enumeration for STDIO adapter.
 */
namespace hermes::adapter::stdio {
/**
 * Define different types of mappers supported by STDIO Adapter.
 * Also define its construction in the MapperFactory.
 */
enum MapperType { BALANCED = 0 /* Balanced Mapping */ };
}  // namespace hermes::adapter::stdio
#endif  // HERMES_STDIO_COMMON_ENUMERATIONS_H
