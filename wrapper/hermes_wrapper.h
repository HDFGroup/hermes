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
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

struct BucketClass;
typedef struct BucketClass BucketClass;

struct VBucketClass;
typedef struct VBucketClass VBucketClass;

int HermesInitHermes(char *hermes_config);

void HermesFinalize();

VBucketClass *HermesVBucketCreate(const char *name);

void HermesVBucketLink(VBucketClass *vbkt, char *blob_name);

bool HermesVBucketIsValid(VBucketClass *vbkt);

BucketClass *HermesBucketCreate(const char *name);

void HermesBucketClose(BucketClass *bkt);

void HermesBucketDestroy(BucketClass *bucket_ptr);

bool HermesBucketContainsBlob(BucketClass *bkt, char *name);

void HermesBucketPut(BucketClass *bkt, char *name, unsigned char *put_data,
                    size_t size);

void HermesBucketGet(BucketClass *bkt, char *blob_name, size_t kPageSize,
                     unsigned char *buf);

#ifdef __cplusplus
}
#endif

#endif
