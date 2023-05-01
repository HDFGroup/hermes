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

#include "hermes_java_wrapper.h"
#include "src_main_Hermes.h"
#include "src_main_Bucket.h"
#include "src/api/hermes.h"
#include <jni.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     src_main_Hermes
 * Method:    create
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_src_main_java_Hermes_create(
    JNIEnv *env, jobject obj) {
  (void) env; (void) obj;
  HERMES->Create(hermes::HermesType::kClient);
}

/*
 * Class:     src_main_Hermes
 * Method:    getBucket
 * Signature: (Ljava/lang/String;)Lsrc/main/Bucket;
 */
JNIEXPORT jobject JNICALL Java_src_main_java_Hermes_getBucket(
    JNIEnv *env, jobject obj, jstring bkt_name_java) {
  (void) obj;
  JavaStringWrap bkt_name(env, bkt_name_java);
  hapi::Bucket bkt = HERMES->GetBucket(bkt_name.data_);
  auto bkt_java = HERMES_JAVA_WRAPPER->ConvertBucketToJava(env, bkt);
  return bkt_java;
}

#ifdef __cplusplus
}
#endif