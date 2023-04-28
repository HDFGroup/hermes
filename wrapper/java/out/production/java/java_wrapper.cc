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

#include "java_wrapper.h"
#include "c/c_wrapper.h"
#include <jni.h>

/** Create Hermes instance */
JNIEXPORT void JNICALL Java_com_hermes_Hermes_create(
    JNIEnv *env, jobject obj) {
  HERMES->Create(hermes::HermesType::kClient);
}

/** Hermes GET bucket */
JNIEXPORT void JNICALL Java_com_hermes_Hermes_get_bucket(
    JNIEnv *env, jobject obj, jstring str) {
}

/** Hermes DESTROY bucket */
JNIEXPORT void JNICALL Java_com_hermes_Hermes_destroy_bucket(
    JNIEnv *env, jobject obj, jstring str) {
}

/** Hermes PUT blob */
JNIEXPORT void JNICALL Java_com_hermes_Hermes_bucket_put_blob(
    JNIEnv *env, jobject obj, jstring str) {
}

/** Hermes GET blob */
JNIEXPORT void JNICALL Java_com_hermes_Hermes_bucket_get_blob(
    JNIEnv *env, jobject obj, jstring str) {
}

/** Hermes DESTROY blob */
JNIEXPORT void JNICALL Java_com_hermes_Hermes_bucket_destroy_blob(
    JNIEnv *env, jobject obj, jstring str) {
}
