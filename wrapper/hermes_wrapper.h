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

#ifndef HERMES_C_WRAPPER_H
#define HERMES_C_WRAPPER_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

struct BucketClass;
typedef struct BucketClass BucketClass;

int HermesInitHermes(char *hermes_config);

BucketClass *HermesBucketCreate(const char *name);

void HermesBucketClose(BucketClass *bkt);

void HermesBucketDestroy(BucketClass *bucket_ptr);

#ifdef __cplusplus
}
#endif

#endif
