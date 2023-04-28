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

#include "src_main_Hermes.h"
#include "c/c_wrapper.h"
#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     src_main_Hermes
 * Method:    create
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_src_main_Hermes_create(JNIEnv *env, jobject obj) {
  HERMES->Create(hermes::HermesType::kClient);
}

/*
 * Class:     src_main_Hermes
 * Method:    getBucket
 * Signature: (Ljava/lang/String;)Lsrc/main/Bucket;
 */
JNIEXPORT jobject JNICALL Java_src_main_Hermes_getBucket(JNIEnv *env, jobject obj, jstring bkt_name) {
}

#ifdef __cplusplus
}
#endif