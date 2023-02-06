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

#ifndef HERMES_SHM_MACROS_H
#define HERMES_SHM_MACROS_H

#define KILOBYTES(n) ((size_t)(n) * (1<<10))
#define MEGABYTES(n) ((size_t)(n) * (1<<20))
#define GIGABYTES(n) ((size_t)(n) * (1<<30))

#define TYPE_BITS(type) ((sizeof(type)*8))

#define TYPE_WRAP(...) (__VA_ARGS__)

#define TYPE_UNWRAP(X) ESC(ISH X)
#define ISH(...) ISH __VA_ARGS__
#define ESC(...) ESC_(__VA_ARGS__)
#define ESC_(...) VAN ## __VA_ARGS__
#define VANISH

#endif  // HERMES_SHM_MACROS_H
