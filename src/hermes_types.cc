//
// Created by lukemartinlogan on 12/5/22.
//

#include "hermes_types.h"
#include "hermes.h"

namespace hermes::api {

////////////////////////////
/// Context Operations
////////////////////////////

/** Default constructor */
Context::Context()
    : policy(HERMES->server_config_.dpe_.default_policy_),
      rr_split(HERMES->server_config_.dpe_.default_rr_split_),
      rr_retry(false),
      disable_swap(false),
      vbkt_id_({0, 0}) {}

////////////////////////////
/// Blob Operations
////////////////////////////

/** Size-based constructor */
Blob::Blob(size_t size) {
  Allocate(HERMES->main_alloc_, size);
}

/** Construct from std::string */
Blob::Blob(const std::string &data) {
  Allocate(HERMES->main_alloc_, data.size());
  memcpy(data_, data.data(), data.size());
}

/** Copy constructor */
Blob::Blob(const Blob &other) {
  if (!Allocate(HERMES->main_alloc_, other.size())) {
    return;
  }
  memcpy(data_, other.data(), size());
}

/** Copy assignment operator */
Blob& Blob::operator=(const Blob &other) {
  if (this != &other) {
    Free();
    if (!Allocate(HERMES->main_alloc_, other.size())) {
      return *this;
    }
    memcpy(data_, other.data(), size());
  }
  return *this;
}

/** Move constructor */
Blob::Blob(Blob &&other) {
  alloc_ = other.alloc_;
  data_ = other.data_;
  size_ = other.size_;
  destructable_ = other.destructable_;
  other.destructable_ = false;
}

/** Move assignment operator */
Blob& Blob::operator=(Blob &other) {
  if (this != &other) {
    Free();
    alloc_ = other.alloc_;
    data_ = other.data_;
    size_ = other.size_;
    destructable_ = other.destructable_;
    other.destructable_ = false;
  }
  return *this;
}

/** Allocate the blob */
bool Blob::Allocate(lipc::Allocator *alloc, size_t size) {
  lipc::OffsetPointer p;
  if (size == 0) {
    alloc_ = nullptr;
    data_ = nullptr;
    size_ = 0;
    destructable_ = false;
    return false;
  }
  alloc_ = alloc;
  data_ = alloc->AllocatePtr<char>(size, p);
  size_ = size;
  destructable_ = true;
  return true;
}

/** Deallocate this blob */
void Blob::Free() {
  if (destructable_ && data_ && size_) {
    alloc_->FreePtr<char>(data_);
  }
}

}  // namespace hermes::api