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

#include "kvstore.h"

namespace hermes::adapter {

/**====================================
 * KVRecord
 * ===================================*/

/** Convert the record from a single array of bytes */
KVRecord::KVRecord(hapi::Blob &blob) {
  BlobSerializer serial(blob);
  while (serial.off_ < blob.size()) {
    std::string field_name;
    hapi::Blob field_val;
    serial >> field_name;
    serial >> field_val;
    record_.emplace(std::move(field_name), std::move(field_val));
  }
}

/** Convert the record into a single array of bytes */
hapi::Blob KVRecord::value() {
  BlobSerializeCounter counter;

  // Get the total size of the serialized blob
  for (auto &[field_name, field_val] : record_) {
    counter << field_name;
    counter << field_val;
  }

  // Produce the serialized blob
  hapi::Blob blob(counter.size_);
  BlobSerializer serial(blob);
  for (auto &[field_name, field_val] : record_) {
    serial << field_name;
    serial << field_val;
  }

  return blob;
}

/**====================================
 * KVTable
 * ===================================*/

/**
 * Create or insert a record into the table
 *
 * @param key the record key
 * @param val the values to update in the record
 * @return None
 * */
void KVTable::Update(const std::string &key, KVRecord &val) {
  hermes::BlobId blob_id;
  bkt_.GetBlobId(key, blob_id);
  if (!blob_id.IsNull()) {
    hapi::Context ctx;
    hapi::Blob record_serial;
    bkt_.LockBlob(blob_id, MdLockType::kExternalWrite);
    bkt_.Get(blob_id, record_serial, ctx);
    KVRecord orig(record_serial);
    for (auto &[field, field_val] : val.record_) {
      orig.record_[field] = field_val;
    }
    bkt_.Put(key, orig.value(), blob_id, ctx);
    bkt_.UnlockBlob(blob_id, MdLockType::kExternalWrite);
  } else {
    hapi::Context ctx;
    hapi::Blob record_serial;
    bkt_.LockBlob(blob_id, MdLockType::kExternalWrite);
    bkt_.Put(key, val.value(), blob_id, ctx);
    bkt_.UnlockBlob(blob_id, MdLockType::kExternalWrite);
  }
}

/**
 * Get a subset of fields from a record
 *
 * @param key the record key
 * @param field the field in the record to update
 * @return The blob containing only the field's data
 * */
KVRecord KVTable::Read(const std::string &key, const KVFieldSet &field) {
  hapi::Blob blob;
  hermes::BlobId blob_id;
  hapi::Context ctx;
  bkt_.GetBlobId(key, blob_id);
  bkt_.Get(blob_id, blob, ctx);
  KVRecord orig_record(blob);
  if (field.size() == 0) {
    return orig_record;
  }

  KVRecord new_record;
  for (const std::string &field_name : field) {
    new_record.record_[field_name] = orig_record.record_[field_name];
  }
  return new_record;
}

/** Delete a record */
void KVTable::Erase(const std::string &key) {
  hapi::Context ctx;
  hermes::BlobId blob_id;
  bkt_.GetBlobId(key, blob_id);
  bkt_.DestroyBlob(blob_id, ctx);
}

/** Destroy this table */
void KVTable::Destroy() {
  bkt_.Destroy();
}

/**====================================
 * KVStore
 * ===================================*/

/** Connect to Hermes */
void KVStore::Connect() {
  HERMES->Create(hermes::HermesType::kClient);
}

/** Get or create a table */
KVTable KVStore::GetTable(const std::string table_name) {
  return KVTable(HERMES->GetBucket(table_name));
}

}  // namespace hermes::adapter