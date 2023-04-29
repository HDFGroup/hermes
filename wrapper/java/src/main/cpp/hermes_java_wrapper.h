//
// Created by lukemartinlogan on 4/28/23.
//

#ifndef HERMES_WRAPPER_JAVA_SRC_MAIN_CPP_HERMES_JAVA_WRAPPER_H_
#define HERMES_WRAPPER_JAVA_SRC_MAIN_CPP_HERMES_JAVA_WRAPPER_H_

#include <jni.h>
#include "hermes.h"

struct JavaStringWrap {
  const char *data_;
  JNIEnv *&env_;
  jstring &string_java_;

  JavaStringWrap(JNIEnv *&env, jstring &string_java)
  : env_(env), string_java_(string_java) {
    data_ = env_->GetStringUTFChars(string_java, nullptr);
  }

  ~JavaStringWrap() {
    env_->ReleaseStringUTFChars(string_java_, data_);
  }
};

class HermesJavaWrapper {
 public:
  /** Convert a C++ UniqueId to Java UniqueId */
  template<typename IdT>
  jobject ConvertUniqueIdToJava(JNIEnv *env,
                                const IdT &id) {
    jclass id_class = env->FindClass("src/main/java/UniqueId");
    jmethodID id_cstor = env->GetMethodID(id_class, "<init>", "(JI)V");
    jobject id_java = env->NewObject(id_class, id_cstor,
                                     id.unique_, id.node_id_);
    return id_java;
  }

  /** Get a C++ UniqueId from Java UniqueId */
  template<typename IdT>
  IdT GetUniqueIdFromJava(JNIEnv *env,
                          jobject bkt_id_java) {
    jclass bkt_id_class = env->GetObjectClass(bkt_id_java);
    jfieldID unique_fid = env->GetFieldID(bkt_id_class, "unique_", "J");
    jfieldID node_id_fid = env->GetFieldID(bkt_id_class, "node_id_", "I");
    IdT tag_id;
    tag_id.unique_ = env->GetLongField(bkt_id_java, unique_fid);
    tag_id.node_id_ = env->GetIntField(bkt_id_java, node_id_fid);
    return tag_id;
  }

  /** Convert a C++ BUCKET to Java Bucket */
  jobject ConvertBucketToJava(JNIEnv *env,
                              hapi::Bucket &bkt) {
    jclass bkt_class = env->FindClass("src/main/java/Bucket");
    jmethodID bkt_constructor = env->GetMethodID(
        bkt_class,
        "<init>",
        "(Lsrc/main/java/UniqueId;)V");
    auto bkt_id_java = ConvertUniqueIdToJava<hermes::TagId>(env, bkt.GetId());
    jobject bkt_java = env->NewObject(bkt_class, bkt_constructor, bkt_id_java);
    return bkt_java;
  }

  /** Get a C++ BUCKET from Java Bucket */
  hapi::Bucket GetBucketFromJava(JNIEnv *env,
                                 jobject bkt_java) {
    jclass bkt_class = env->GetObjectClass(bkt_java);
    jfieldID bkt_id_fid = env->GetFieldID(bkt_class, "bkt_id_",
                                          "Lsrc/main/java/UniqueId;");
    jobject bkt_id_java = env->GetObjectField(bkt_java, bkt_id_fid);
    auto tag_id = GetUniqueIdFromJava<hermes::TagId>(env, bkt_id_java);
    return hapi::Bucket(tag_id);
  }

  /** Convert a C++ BLOB to Java Blob */
  jobject ConvertBlobToJava(JNIEnv *env,
                            hapi::Blob &blob) {
    // Prepare data to place into Java Blob
    blob.destructable_ = false;
    jobject data_java = env->NewDirectByteBuffer(blob.data(), blob.size());
    hipc::Allocator *alloc = blob.GetAllocator();

    // Allocate new Blob java
    jclass blob_class = env->FindClass("src/main/java/Blob");
    jmethodID blob_cstor = env->GetMethodID(
        blob_class,
        "<init>",
        "(Ljava/nio/ByteBuffer;JJJ)V");
    jobject blob_java = env->NewObject(blob_class, blob_cstor,
                                       data_java,
                                       (uint64_t)blob.data(),
                                       (uint64_t)blob.size(),
                                       (uint64_t)alloc);
    return blob_java;
  }

  /** Get a C++ BLOB from Java Blob */
  hapi::Blob GetBlobFromJava(JNIEnv *env,
                             jobject blob_java) {
    jclass blob_class = env->GetObjectClass(blob_java);
    jfieldID blob_data_fid = env->GetFieldID(blob_class, "data_ptr_", "J");
    jfieldID blob_size_fid = env->GetFieldID(blob_class, "size_", "J");
    jfieldID blob_alloc_fid = env->GetFieldID(blob_class, "alloc_", "J");
    hapi::Blob blob;
    blob.data_ = reinterpret_cast<char*>(
        env->GetLongField(blob_java, blob_data_fid));
    blob.size_ = env->GetLongField(blob_java, blob_size_fid);
    blob.alloc_ = reinterpret_cast<hipc::Allocator*>(
        env->GetLongField(blob_java, blob_alloc_fid));
    return blob;
  }
};

#define HERMES_JAVA_WRAPPER \
  hshm::EasySingleton<HermesJavaWrapper>::GetInstance()

#endif //HERMES_WRAPPER_JAVA_SRC_MAIN_CPP_HERMES_JAVA_WRAPPER_H_
