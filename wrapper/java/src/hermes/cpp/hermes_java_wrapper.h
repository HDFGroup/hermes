//
// Created by lukemartinlogan on 4/28/23.
//

#ifndef HERMES_WRAPPER_Java_src_hermes_CPP_HERMES_JAVA_WRAPPER_H_
#define HERMES_WRAPPER_Java_src_hermes_CPP_HERMES_JAVA_WRAPPER_H_

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
  JNIEnv *env_;
  /** UniqueId Java class */
  jclass id_class;
  jmethodID id_cstor;
  jfieldID unique_fid;
  jfieldID node_id_fid;

  /** Bucket Java class */
  jclass bkt_class;
  jmethodID bkt_constructor;
  jfieldID bkt_id_fid;

  /** Blob java class */
  jclass blob_class;
  jmethodID blob_cstor;
  jfieldID blob_data_fid;
  jfieldID blob_size_fid;
  jfieldID blob_alloc_fid;
  jfieldID is_native_fid;

  /** Blob ID vector methods */
  jclass vector_class;
  jmethodID vector_cstor;
  jmethodID vector_add;

 public:
  HermesJavaWrapper(JNIEnv *env) : env_(env) {
    /* UniqueId methods */
    id_class = FindClass("hermes/java/UniqueId");
    id_cstor = env->GetMethodID(id_class, "<init>", "(JI)V");
    unique_fid = env->GetFieldID(id_class, "unique_", "J");
    node_id_fid = env->GetFieldID(id_class, "node_id_", "I");

    /* Bucket methods */
    bkt_class = FindClass("hermes/java/Bucket");
    bkt_id_fid = env->GetFieldID(bkt_class, "bkt_id_",
                                 "Lhermes/java/UniqueId;");
    bkt_constructor = env->GetMethodID(
        bkt_class,
        "<init>",
        "(Lhermes/java/UniqueId;)V");

    /* Blob methods */
    blob_class = FindClass("hermes/java/Blob");
    blob_cstor = env->GetMethodID(
        blob_class,
        "<init>",
        "(Ljava/nio/ByteBuffer;IJ)V");
    blob_data_fid = env->GetFieldID(blob_class, "data_", "Ljava/nio/ByteBuffer;");
    blob_size_fid = env->GetFieldID(blob_class, "size_", "I");
    blob_alloc_fid = env->GetFieldID(blob_class, "alloc_", "J");
    is_native_fid = env->GetFieldID(blob_class, "is_native_", "Z");

    /* Blob vector methods */
    vector_class = FindClass("java/util/Vector");
    vector_cstor = env->
        GetMethodID(vector_class, "<init>", "(I)V");
    vector_add = env->GetMethodID(
        vector_class, "add", "(Ljava/lang/Object;)Z");
  }

  HSHM_ALWAYS_INLINE jclass FindClass(const std::string &java_class) {
    return (jclass)env_->NewGlobalRef(env_->FindClass(java_class.c_str()));
  }

  /** Convert a C++ UniqueId to Java UniqueId */
  template<typename IdT>
  jobject ConvertUniqueIdToJava(JNIEnv *env,
                                const IdT &id) {
    jobject id_java = env->NewObject(id_class, id_cstor,
                                     id.unique_, id.node_id_);
    return id_java;
  }

  /** Get a C++ UniqueId from Java UniqueId */
  template<typename IdT>
  IdT GetUniqueIdFromJava(JNIEnv *env,
                          jobject bkt_id_java) {
    IdT tag_id;
    tag_id.unique_ = env->GetLongField(bkt_id_java, unique_fid);
    tag_id.node_id_ = env->GetIntField(bkt_id_java, node_id_fid);
    return tag_id;
  }

  /** Convert a C++ BUCKET to Java Bucket */
  jobject ConvertBucketToJava(JNIEnv *env,
                              hapi::Bucket &bkt) {
    auto bkt_id_java = ConvertUniqueIdToJava<hermes::TagId>(env, bkt.GetId());
    jobject bkt_java = env->NewObject(bkt_class, bkt_constructor, bkt_id_java);
    return bkt_java;
  }

  /** Get a C++ BUCKET from Java Bucket */
  hapi::Bucket GetBucketFromJava(JNIEnv *env,
                                 jobject bkt_java) {
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
    jobject blob_java = env->NewObject(blob_class, blob_cstor,
                                       data_java,
                                       (jint)blob.size(),
                                       (jlong)alloc);
    return blob_java;
  }

  /** Get a C++ BLOB from Java Blob */
  hapi::Blob GetBlobFromJava(JNIEnv *env,
                             jobject blob_java) {
    hapi::Blob blob;
    bool is_native = env->GetBooleanField(blob_java, is_native_fid);
    jobject data = env->GetObjectField(blob_java, blob_data_fid);
    blob.data_ = reinterpret_cast<char*>(
        env->GetDirectBufferAddress(data));
    blob.size_ = env->GetIntField(blob_java, blob_size_fid);
    if (is_native) {
      blob.alloc_ = reinterpret_cast<hipc::Allocator *>(
          env->GetLongField(blob_java, blob_alloc_fid));
    }
    return blob;
  }

  /** Convert a blob ID vector to a Java vector */
  jobject ConvertBlobIdVectorToJava(JNIEnv *env,
                                    std::vector<hermes::BlobId> &blob_ids) {
    // Allocate new Blob java
    jobject blob_ids_java = env->NewObject(vector_class, vector_cstor,
                                           (jint)blob_ids.size());
    for (hermes::BlobId &blob_id : blob_ids) {
      jobject blob_id_java = ConvertUniqueIdToJava(env, blob_id);
      env->CallBooleanMethod(blob_ids_java, vector_add, blob_id_java);
    }
    return blob_ids_java;
  }
};

#define HERMES_JAVA_WRAPPER \
  hshm::EasySingleton<HermesJavaWrapper>::GetInstance(env)

#endif //HERMES_WRAPPER_Java_src_hermes_CPP_HERMES_JAVA_WRAPPER_H_
