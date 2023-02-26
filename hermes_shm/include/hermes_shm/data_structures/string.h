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


#ifndef HERMES_DATA_STRUCTURES_LOCKLESS_STRING_H_
#define HERMES_DATA_STRUCTURES_LOCKLESS_STRING_H_

#include "internal/shm_internal.h"
#include <string>

namespace hermes_shm::ipc {

/** forward declaration for string */
class string;

/**
 * MACROS used to simplify the string namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME string
#define TYPED_CLASS string
#define TYPED_HEADER ShmHeader<string>

/** string shared-memory header */
template<>
struct ShmHeader<string> : public ShmBaseHeader {
  size_t length_;
  AtomicPointer text_;

  /** Default constructor */
  ShmHeader() = default;

  /** Copy constructor */
  ShmHeader(const ShmHeader &other) {
    strong_copy(other);
  }

  /** Copy assignment operator */
  ShmHeader& operator=(const ShmHeader &other) {
    if (this != &other) {
      strong_copy(other);
    }
    return *this;
  }

  /** Strong copy operation */
  void strong_copy(const ShmHeader &other) {
    length_ = other.length_;
    text_ = other.text_;
  }

  /** Move constructor */
  ShmHeader(ShmHeader &&other) {
    weak_move(other);
  }

  /** Move operator */
  ShmHeader& operator=(ShmHeader &&other) {
    if (this != &other) {
      weak_move(other);
    }
    return *this;
  }

  /** Move operation */
  void weak_move(ShmHeader &other) {
    strong_copy(other);
  }
};

/**
 * A string of characters.
 * */
class string : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))

 public:
  char *text_;
  size_t length_;

 public:
  ////////////////////////////
  /// SHM Overrides
  ////////////////////////////

  /** Default constructor */
  string() : length_(0) {}

  /** Default shm constructor */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
  }

  /** Construct from a C-style string with allocator in shared memory */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc, const char *text) {
    size_t length = strlen(text);
    shm_init_main(header, alloc, length);
    _create_str(text, length);
  }

  /** Construct for an std::string with allocator in shared-memory */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc, const std::string &text) {
    shm_init_main(header, alloc, text.size());
    _create_str(text.data(), text.size());
  }

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          Allocator *alloc, string &other) {
    shm_init_main(header, alloc);
    header_->length_ = other.header_->length_;
    header_->text_ = other.header_->text_;
    shm_deserialize_main();
  }

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            Allocator *alloc, const string &other) {
    shm_init_main(header, alloc, other.size());
    _create_str(other.data(), other.size());
    shm_deserialize_main();
  }

  /** Construct by concatenating two string in shared-memory */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc,
                     const string &text1, const string &text2) {
    size_t length = text1.size() + text2.size();
    shm_init_main(header, alloc, length);
    memcpy(text_,
           text1.data(), text1.size());
    memcpy(text_ + text1.size(),
           text2.data(), text2.size());
    text_[length] = 0;
  }

  /**
   * Construct a string of specific length and allocator in shared memory
   * */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc, size_t length) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    text_ = alloc_->template
      AllocatePtr<char>(
        length + 1,
        header_->text_);
    header_->length_ = length;
    length_ = length;
    text_[length] = 0;
  }

  /**
   * Destroy the shared-memory data.
   * */
  void shm_destroy_main() {
    if (length_) {
      alloc_->Free(header_->text_);
      length_ = 0;
    }
  }

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {
    text_ = alloc_->template
      Convert<char>(header_->text_);
    length_ = header_->length_;
  }

  ////////////////////////////
  /// String Operations
  ////////////////////////////

  /** Get character at index i in the string */
  char& operator[](size_t i) const {
    return text_[i];
  }

  /** Convert into a std::string */
  std::string str() const {
    return {text_, length_};
  }

  /** Add two strings together */
  string operator+(const std::string &other) {
    string tmp(other);
    return string(GetAllocator(), *this, tmp);
  }

  /** Add two strings together */
  string operator+(const string &other) {
    return string(GetAllocator(), *this, other);
  }

  /** Get the size of the current string */
  size_t size() const {
    return length_;
  }

  /** Get a constant reference to the C-style string */
  char* c_str() const {
    return text_;
  }

  /** Get a constant reference to the C-style string */
  char* data() const {
    return text_;
  }

  /** Get a mutable reference to the C-style string */
  char* data_mutable() {
    return text_;
  }

  /**
   * Comparison operators
   * */

  int _strncmp(const char *a, size_t len_a,
               const char *b, size_t len_b) const {
    if (len_a != len_b) {
      return int((int64_t)len_a - (int64_t)len_b);
    }
    int sum = 0;
    for (size_t i = 0; i < len_a; ++i) {
      sum += a[i] - b[i];
    }
    return sum;
  }

#define HERMES_STR_CMP_OPERATOR(op) \
  bool operator op(const char *other) const { \
    return _strncmp(data(), size(), other, strlen(other)) op 0; \
  } \
  bool operator op(const std::string &other) const { \
    return _strncmp(data(), size(), other.data(), other.size()) op 0; \
  } \
  bool operator op(const string &other) const { \
    return _strncmp(data(), size(), other.data(), other.size()) op 0; \
  }

  HERMES_STR_CMP_OPERATOR(==)
  HERMES_STR_CMP_OPERATOR(!=)
  HERMES_STR_CMP_OPERATOR(<)
  HERMES_STR_CMP_OPERATOR(>)
  HERMES_STR_CMP_OPERATOR(<=)
  HERMES_STR_CMP_OPERATOR(>=)

#undef HERMES_STR_CMP_OPERATOR

 private:
  inline void _create_str(const char *text, size_t length) {
    memcpy(text_, text, length);
    text_[length] = 0;
  }
};

/** Consider the string as an uniterpreted set of bytes */
typedef string charbuf;

}  // namespace hermes_shm::ipc

namespace std {

/** Hash function for string */
template<>
struct hash<hermes_shm::ipc::string> {
  size_t operator()(const hermes_shm::ipc::string &text) const {
    size_t sum = 0;
    for (size_t i = 0; i < text.size(); ++i) {
      auto shift = static_cast<size_t>(i % sizeof(size_t));
      auto c = static_cast<size_t>((unsigned char)text[i]);
      sum = 31*sum + (c << shift);
    }
    return sum;
  }
};

}  // namespace std

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_DATA_STRUCTURES_LOCKLESS_STRING_H_
