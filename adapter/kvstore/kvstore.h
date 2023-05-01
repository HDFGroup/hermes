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

#ifndef HERMES_ADAPTER_KVSTORE_KVSTORE_H_
#define HERMES_ADAPTER_KVSTORE_KVSTORE_H_

#include <string>
#include <unordered_map>
#include "hermes.h"

namespace hermes::adapter {

/** Denotes the set of fields for a record */
typedef std::unordered_set<std::string> KVFieldSet;

/** Measure the serialized size */
struct BlobSerializeCounter {
  size_t size_;

  /** Constructor */
  BlobSerializeCounter() : size_(0) {}

  /** Serialize std::string into blob */
  BlobSerializeCounter& operator<<(const std::string &data) {
    (*this) << data.size();
    size_ += data.size();
    return *this;
  }

  /** Serialize blob into blob */
  BlobSerializeCounter& operator<<(const hapi::Blob &data) {
    (*this) << data.size();
    size_ += data.size();
    return *this;
  }

  /** Serialize size_t into blob */
  BlobSerializeCounter& operator<<(const size_t &data) {
    size_ += sizeof(size_t);
    return *this;
  }
};

/** Serialize data into and out of blob */
struct BlobSerializer {
  hapi::Blob &blob_;
  size_t off_;

  /** Constructor */
  explicit BlobSerializer(hapi::Blob &blob) : blob_(blob), off_(0) {}

  /** Serialize a string type */
  template<typename StringT>
  void SerializeString(const StringT &data) {
    (*this) << data.size();
    memcpy(blob_.data() + off_, data.data(), data.size());
    off_ += data.size();
  }

  /** Deserialize a string type */
  template<typename StringT>
  void DeserializeString(StringT &data) {
    size_t size;
    (*this) >> size;
    data.resize(size);
    memcpy(data.data(), blob_.data() + off_, data.size());
    off_ += data.size();
  }

  /** Serialize std::string into blob */
  BlobSerializer& operator<<(const std::string &data) {
    SerializeString<std::string>(data);
    return *this;
  }

  /** Deserialize std::string from blob */
  BlobSerializer& operator>>(std::string &data) {
    DeserializeString<std::string>(data);
    return *this;
  }

  /** Serialize blob into blob */
  BlobSerializer& operator<<(const hapi::Blob &data) {
    SerializeString<hapi::Blob>(data);
    return *this;
  }

  /** Deserialize blob from blob */
  BlobSerializer& operator>>(hapi::Blob &data) {
    DeserializeString<hapi::Blob>(data);
    return *this;
  }

  /** Serialize size_t into blob */
  BlobSerializer& operator<<(const size_t &data) {
    memcpy(blob_.data() + off_, &data, sizeof(size_t));
    off_ += sizeof(size_t);
    return *this;
  }

  /** Deserialize size_t from blob */
  BlobSerializer& operator>>(size_t &data) {
    memcpy(&data, blob_.data() + off_, sizeof(size_t));
    off_ += sizeof(size_t);
    return *this;
  }
};

/** Denotes a record to place in a table */
struct KVRecord {
  std::unordered_map<std::string, hapi::Blob> record_;

  /** Default constructor */
  KVRecord() = default;

  /** Convert the record from a single array of bytes */
  explicit KVRecord(hapi::Blob &blob);

  /** Convert the record into a single array of bytes */
  hapi::Blob value();
};

/** Denotes a set of records */
class KVTable {
 public:
  hapi::Bucket bkt_;

  /** Emplace Constructor */
  explicit KVTable(const hapi::Bucket &bkt) : bkt_(bkt) {}

  /**
   * Create or insert a record into the table
   *
   * @param key the record key
   * @param val the values to update in the record
   * @return None
   * */
  void Update(const std::string &key, KVRecord &val);

  /**
   * Get a subset of fields from a record
   *
   * @param key the record key
   * @param field the field in the record to update
   * @return The blob containing only the field's data
   * */
  KVRecord Read(const std::string &key, const KVFieldSet &field);

  /** Delete a record */
  void Erase(const std::string &key);

  /** Destroy this table */
  void Destroy();
};

/** The key-value store instance */
class KVStore {
 public:
  /** Connect to Hermes */
  void Connect();

  /** Get or create a table */
  KVTable GetTable(const std::string table_name);
};

}  // namespace hermes::adapter

#endif  // HERMES_ADAPTER_KVSTORE_KVSTORE_H_
