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

#ifndef HERMES_SHM_DATA_STRUCTURES_LOCKLESS_VECTOR_H_
#define HERMES_SHM_DATA_STRUCTURES_LOCKLESS_VECTOR_H_

#include "hermes_shm/data_structures/data_structure.h"
#include "hermes_shm/data_structures/internal/shm_archive_or_t.h"

#include <vector>

namespace hermes_shm::ipc {

/** forward pointer for vector */
template<typename T>
class vector;

/**
 * The vector iterator implementation
 * */
template<typename T, bool FORWARD_ITER>
struct vector_iterator_templ {
 public:
  lipc::ShmRef<vector<T>> vec_;
  off64_t i_;

  /** Default constructor */
  vector_iterator_templ() = default;

  /** Construct an iterator */
  inline explicit vector_iterator_templ(TypedPointer<vector<T>> vec)
  : vec_(vec) {}

  /** Construct end iterator */
  inline explicit vector_iterator_templ(size_t i)
  : i_(static_cast<off64_t>(i)) {}

  /** Construct an iterator at \a i offset */
  inline explicit vector_iterator_templ(TypedPointer<vector<T>> vec, size_t i)
  : vec_(vec), i_(static_cast<off64_t>(i)) {}

  /** Construct an iterator at \a i offset */
  inline explicit vector_iterator_templ(const lipc::ShmRef<vector<T>> &vec,
                                        size_t i)
  : vec_(vec), i_(static_cast<off64_t>(i)) {}

  /** Copy constructor */
  inline vector_iterator_templ(const vector_iterator_templ &other)
  : vec_(other.vec_), i_(other.i_) {}

  /** Copy assignment operator  */
  inline vector_iterator_templ&
  operator=(const vector_iterator_templ &other) {
    if (this != &other) {
      vec_ = other.vec_;
      i_ = other.i_;
    }
    return *this;
  }

  /** Move constructor */
  inline vector_iterator_templ(vector_iterator_templ &&other) {
    vec_ = other.vec_;
    i_ = other.i_;
  }

  /** Move assignment operator  */
  inline vector_iterator_templ&
  operator=(vector_iterator_templ &&other) {
    if (this != &other) {
      vec_ = other.vec_;
      i_ = other.i_;
    }
    return *this;
  }

  /** Dereference the iterator */
  inline ShmRef<T> operator*() {
    return ShmRef<T>(vec_->data_ar()[i_].internal_ref(
      vec_->GetAllocator()));
  }

  /** Dereference the iterator */
  inline const ShmRef<T> operator*() const {
    return ShmRef<T>(vec_->data_ar_const()[i_].internal_ref(
      vec_->GetAllocator()));
  }

  /** Increment iterator in-place */
  inline vector_iterator_templ& operator++() {
    if (is_end()) { return *this; }
    if constexpr(FORWARD_ITER) {
      ++i_;
      if (i_ >= vec_->size()) {
        set_end();
      }
    } else {
      if (i_ == 0) {
        set_end();
      } else {
        --i_;
      }
    }
    return *this;
  }

  /** Decrement iterator in-place */
  inline vector_iterator_templ& operator--() {
    if (is_begin() || is_end()) { return *this; }
    if constexpr(FORWARD_ITER) {
      --i_;
    } else {
      ++i_;
    }
    return *this;
  }

  /** Create the next iterator */
  inline vector_iterator_templ operator++(int) const {
    vector_iterator_templ next_iter(*this);
    ++next_iter;
    return next_iter;
  }

  /** Create the prior iterator */
  inline vector_iterator_templ operator--(int) const {
    vector_iterator_templ prior_iter(*this);
    --prior_iter;
    return prior_iter;
  }

  /** Increment iterator by \a count and return */
  inline vector_iterator_templ operator+(size_t count) const {
    if (is_end()) { return end(); }
    if constexpr(FORWARD_ITER) {
      if (i_ + count > vec_->size()) {
        return end();
      }
      return vector_iterator_templ(vec_, i_ + count);
    } else {
      if (i_ < count - 1) {
        return end();
      }
      return vector_iterator_templ(vec_, i_ - count);
    }
  }

  /** Decrement iterator by \a count and return */
  inline vector_iterator_templ operator-(size_t count) const {
    if (is_end()) { return end(); }
    if constexpr(FORWARD_ITER) {
      if (i_ < count) {
        return begin();
      }
      return vector_iterator_templ(vec_, i_ - count);
    } else {
      if (i_ + count > vec_->size() - 1) {
        return begin();
      }
      return vector_iterator_templ(vec_, i_ + count);
    }
  }

  /** Increment iterator by \a count in-place */
  inline void operator+=(size_t count) {
    if (is_end()) { return end(); }
    if constexpr(FORWARD_ITER) {
      if (i_ + count > vec_->size()) {
        set_end();
        return;
      }
      i_ += count;
      return;
    } else {
      if (i_ < count - 1) {
        set_end();
        return;
      }
      i_ -= count;
      return;
    }
  }

  /** Decrement iterator by \a count in-place */
  inline void operator-=(size_t count) {
    if (is_end()) { return end(); }
    if constexpr(FORWARD_ITER) {
      if (i_ < count) {
        set_begin();
        return;
      }
      i_ -= count;
      return;
    } else {
      if (i_ + count > vec_->size() - 1) {
        set_begin();
        return;
      }
      i_ += count;
      return;
    }
  }

  /** Check if two iterators are equal */
  inline friend bool operator==(const vector_iterator_templ &a,
                         const vector_iterator_templ &b) {
    return (a.i_ == b.i_);
  }

  /** Check if two iterators are inequal */
  inline friend bool operator!=(const vector_iterator_templ &a,
                         const vector_iterator_templ &b) {
    return (a.i_ != b.i_);
  }

  /** Create the begin iterator */
  inline vector_iterator_templ const begin() {
    if constexpr(FORWARD_ITER) {
      return vector_iterator_templ(vec_, 0);
    } else {
      return vector_iterator_templ(vec_, vec_->size() - 1);
    }
  }

  /** Create the end iterator */
  inline static vector_iterator_templ const end() {
    static vector_iterator_templ end_iter(-1);
    return end_iter;
  }

  /** Set this iterator to end */
  inline void set_end() {
    i_ = -1;
  }

  /** Set this iterator to begin */
  inline void set_begin() {
    if constexpr(FORWARD_ITER) {
      if (vec_->size() > 0) {
        i_ = 0;
      } else {
        set_end();
      }
    } else {
      i_ = vec_->size() - 1;
    }
  }

  /** Determine whether this iterator is the begin iterator */
  inline bool is_begin() const {
    if constexpr(FORWARD_ITER) {
      return (i_ == 0);
    } else {
      return (i_ == vec_->size() - 1);
    }
  }

  /** Determine whether this iterator is the end iterator */
  inline bool is_end() const {
    return i_ < 0;
  }
};

/** Forward iterator typedef */
template<typename T>
using vector_iterator = vector_iterator_templ<T, true>;

/** Backward iterator typedef */
template<typename T>
using vector_riterator = vector_iterator_templ<T, false>;

/** Constant forward iterator typedef */
template<typename T>
using vector_citerator = vector_iterator_templ<T, true>;

/** Backward iterator typedef */
template<typename T>
using vector_criterator = vector_iterator_templ<T, false>;

/**
 * MACROS used to simplify the vector namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME vector
#define TYPED_CLASS vector<T>
#define TYPED_HEADER ShmHeader<vector<T>>

/**
 * The vector shared-memory header
 * */
template<typename T>
struct ShmHeader<TYPED_CLASS> : public ShmBaseHeader {
  AtomicPointer vec_ptr_;
  size_t max_length_, length_;

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
    vec_ptr_ = other.vec_ptr_;
    max_length_ = other.max_length_;
    length_ = other.length_;
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
 * The vector class
 * */
template<typename T>
class vector : public ShmContainer {
 public:
 SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))

 public:
  ////////////////////////////
  /// SHM Overrides
  ////////////////////////////

  /** Default constructor */
  vector() = default;

  /** Construct the vector in shared memory */
  template<typename ...Args>
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc, size_t length, Args&& ...args) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    resize(length, std::forward<Args>(args)...);
  }

  /** Construct the vector in shared memory */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    header_->length_ = 0;
    header_->max_length_ = 0;
    header_->vec_ptr_.SetNull();
  }

  /** Copy from std::vector */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc, std::vector<T> &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    reserve(other.size());
    for (auto &entry : other) {
      emplace_back(entry);
    }
  }

  /** Destroy all shared memory allocated by the vector */
  void shm_destroy_main() {
    erase(begin(), end());
    if (!header_->vec_ptr_.IsNull()) {
      alloc_->Free(header_->vec_ptr_);
    }
  }

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          Allocator *alloc, vector &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    *header_ = *(other.header_);
    other.header_->length_ = 0;
  }

  /** Copy a vector */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            Allocator *alloc, const vector &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    reserve(other.size());
    for (auto iter = other.cbegin(); iter != other.cend(); ++iter) {
      emplace_back((**iter));
    }
  }

  ////////////////////////////
  /// Vector Operations
  ////////////////////////////

  /**
   * Convert to std::vector
   * */
  std::vector<T> vec() {
    std::vector<T> v;
    v.reserve(size());
    for (lipc::ShmRef<T> entry : *this) {
      v.emplace_back(*entry);
    }
    return v;
  }

  /**
   * Reserve space in the vector to emplace elements. Does not
   * change the size of the list.
   *
   * @param length the maximum size the vector can get before a growth occurs
   * @param args the arguments to construct
   * */
  template<typename ...Args>
  void reserve(size_t length, Args&& ...args) {
    if (IsNull()) { shm_init(); }
    if (length == 0) { return; }
    grow_vector(data_ar(), length, false, std::forward<Args>(args)...);
  }

  /**
   * Reserve space in the vector to emplace elements. Changes the
   * size of the list.
   *
   * @param length the maximum size the vector can get before a growth occurs
   * @param args the arguments used to construct the vector elements
   * */
  template<typename ...Args>
  void resize(size_t length, Args&& ...args) {
    if (IsNull()) { shm_init(); }
    if (length == 0) { return; }
    grow_vector(data_ar(), length, true, std::forward<Args>(args)...);
    header_->length_ = length;
  }

  /** Index the vector at position i */
  lipc::ShmRef<T> operator[](const size_t i) {
    ShmHeaderOrT<T> *vec = data_ar();
    return lipc::ShmRef<T>(vec[i].internal_ref(alloc_));
  }

  /** Get first element of vector */
  lipc::ShmRef<T> front() {
    return (*this)[0];
  }

  /** Get last element of vector */
  lipc::ShmRef<T> back() {
    return (*this)[size() - 1];
  }

  /** Index the vector at position i */
  const lipc::ShmRef<T> operator[](const size_t i) const {
    ShmHeaderOrT<T> *vec = data_ar_const();
    return lipc::ShmRef<T>(vec[i].internal_ref(alloc_));
  }

  /** Construct an element at the back of the vector */
  template<typename... Args>
  void emplace_back(Args&& ...args) {
    ShmHeaderOrT<T> *vec = data_ar();
    if (header_->length_ == header_->max_length_) {
      vec = grow_vector(vec, 0, false);
    }
    Allocator::ConstructObj<ShmHeaderOrT<T>>(
      *(vec + header_->length_),
      alloc_, std::forward<Args>(args)...);
    ++header_->length_;
  }

  /** Construct an element in the front of the vector */
  template<typename ...Args>
  void emplace_front(Args&& ...args) {
    emplace(begin(), std::forward<Args>(args)...);
  }

  /** Construct an element at an arbitrary position in the vector */
  template<typename ...Args>
  void emplace(vector_iterator<T> pos, Args&&... args) {
    if (pos.is_end()) {
      emplace_back(std::forward<Args>(args)...);
      return;
    }
    ShmHeaderOrT<T> *vec = data_ar();
    if (header_->length_ == header_->max_length_) {
      vec = grow_vector(vec, 0, false);
    }
    shift_right(pos);
    Allocator::ConstructObj<ShmHeaderOrT<T>>(
      *(vec + pos.i_),
      alloc_, std::forward<Args>(args)...);
    ++header_->length_;
  }

  /** Delete the element at \a pos position */
  void erase(vector_iterator<T> pos) {
    if (pos.is_end()) return;
    shift_left(pos, 1);
    header_->length_ -= 1;
  }

  /** Delete elements between first and last  */
  void erase(vector_iterator<T> first, vector_iterator<T> last) {
    size_t last_i;
    if (first.is_end()) return;
    if (last.is_end()) {
      last_i = size();
    } else {
      last_i = last.i_;
    }
    size_t count = last_i - first.i_;
    if (count == 0) return;
    shift_left(first, count);
    header_->length_ -= count;
  }

  /** Delete all elements from the vector */
  void clear() {
    erase(begin(), end());
  }

  /** Get the size of the vector */
  size_t size() const {
    if (header_ == nullptr) {
      return 0;
    }
    return header_->length_;
  }

  /** Get the data in the vector */
  void* data() {
    if (header_ == nullptr) {
      return nullptr;
    }
    return alloc_->template
      Convert<void>(header_->vec_ptr_);
  }

  /** Get constant pointer to the data */
  void* data_const() const {
    if (header_ == nullptr) {
      return nullptr;
    }
    return alloc_->template
      Convert<void>(header_->vec_ptr_);
  }

  /**
   * Retreives a pointer to the array from the process-independent pointer.
   * */
  ShmHeaderOrT<T>* data_ar() {
    return alloc_->template
      Convert<ShmHeaderOrT<T>>(header_->vec_ptr_);
  }

  /**
   * Retreives a pointer to the array from the process-independent pointer.
   * */
  ShmHeaderOrT<T>* data_ar_const() const {
    return alloc_->template
      Convert<ShmHeaderOrT<T>>(header_->vec_ptr_);
  }

 private:
  /**
   * Grow a vector to a new size.
   *
   * @param vec the C-style array of elements to grow
   * @param max_length the new length of the vector. If 0, the current size
   * of the vector will be multiplied by a constant.
   * @param args the arguments used to construct the elements of the vector
   * */
  template<typename ...Args>
  ShmHeaderOrT<T>* grow_vector(ShmHeaderOrT<T> *vec, size_t max_length,
                                   bool resize, Args&& ...args) {
    // Grow vector by 25%
    if (max_length == 0) {
      max_length = 5 * header_->max_length_ / 4;
      if (max_length <= header_->max_length_ + 10) {
        max_length += 10;
      }
    }
    if (max_length < header_->max_length_) {
      return nullptr;
    }

    // Allocate new shared-memory vec
    ShmHeaderOrT<T> *new_vec;
    if constexpr(std::is_pod<T>() || IS_SHM_ARCHIVEABLE(T)) {
      // Use reallocate for well-behaved objects
      new_vec = alloc_->template
        ReallocateObjs<ShmHeaderOrT<T>>(header_->vec_ptr_, max_length);
    } else {
      // Use std::move for unpredictable objects
      Pointer new_p;
      new_vec = alloc_->template
        AllocateObjs<ShmHeaderOrT<T>>(max_length, new_p);
      for (size_t i = 0; i < header_->length_; ++i) {
        lipc::ShmRef<T> old = (*this)[i];
        Allocator::ConstructObj<ShmHeaderOrT<T>>(
          *(new_vec + i),
          alloc_, std::move(*old));
      }
      if (!header_->vec_ptr_.IsNull()) {
        alloc_->Free(header_->vec_ptr_);
      }
      header_->vec_ptr_ = new_p;
    }
    if (new_vec == nullptr) {
      throw OUT_OF_MEMORY.format("vector::emplace_back",
                                 max_length*sizeof(ShmHeaderOrT<T>));
    }
    if (resize) {
      for (size_t i = header_->length_; i < max_length; ++i) {
        Allocator::ConstructObj<ShmHeaderOrT<T>>(
          *(new_vec + i),
          alloc_, std::forward<Args>(args)...);
      }
    }

    // Update vector header
    header_->max_length_ = max_length;

    return new_vec;
  }

  /**
   * Shift every element starting at "pos" to the left by count. Any element
   * who would be shifted before "pos" will be deleted.
   *
   * @param pos the starting position
   * @param count the amount to shift left by
   * */
  void shift_left(const vector_iterator<T> pos, int count = 1) {
    ShmHeaderOrT<T> *vec = data_ar();
    for (int i = 0; i < count; ++i) {
      auto &vec_i = *(vec + pos.i_ + i);
      vec_i.shm_destroy(alloc_);
      Allocator::DestructObj<ShmHeaderOrT<T>>(vec_i);
    }
    auto dst = vec + pos.i_;
    auto src = dst + count;
    for (auto i = pos.i_ + count; i < size(); ++i) {
      memcpy(dst, src, sizeof(ShmHeaderOrT<T>));
      dst += 1; src += 1;
    }
  }

  /**
   * Shift every element starting at "pos" to the right by count. Increases
   * the total number of elements of the vector by "count". Does not modify
   * the size parameter of the vector, this is done elsewhere.
   *
   * @param pos the starting position
   * @param count the amount to shift right by
   * */
  void shift_right(const vector_iterator<T> pos, int count = 1) {
    auto src = data_ar() + size() - 1;
    auto dst = src + count;
    auto sz = static_cast<off64_t>(size());
    for (auto i = sz - 1; i >= pos.i_; --i) {
      memcpy(dst, src, sizeof(ShmHeaderOrT<T>));
      dst -= 1; src -= 1;
    }
  }

  ////////////////////////////
  /// Iterators
  ////////////////////////////

 public:
  /** Beginning of the forward iterator */
  vector_iterator<T> begin() {
    if (size() == 0) { return end(); }
    vector_iterator<T> iter(GetShmPointer<TypedPointer<vector<T>>>());
    iter.set_begin();
    return iter;
  }

  /** End of the forward iterator */
  static vector_iterator<T> const end() {
    return vector_iterator<T>::end();
  }

  /** Beginning of the constant forward iterator */
  vector_citerator<T> cbegin() const {
    if (size() == 0) { return cend(); }
    vector_citerator<T> iter(GetShmPointer<TypedPointer<vector<T>>>());
    iter.set_begin();
    return iter;
  }

  /** End of the forward iterator */
  static const vector_citerator<T> cend() {
    return vector_citerator<T>::end();
  }

  /** Beginning of the reverse iterator */
  vector_riterator<T> rbegin() {
    if (size() == 0) { return rend(); }
    vector_riterator<T> iter(GetShmPointer<TypedPointer<vector<T>>>());
    iter.set_begin();
    return iter;
  }

  /** End of the reverse iterator */
  static vector_riterator<T> const rend() {
    return vector_riterator<T>::end();
  }

  /** Beginning of the constant reverse iterator */
  vector_criterator<T> crbegin() {
    if (size() == 0) { return rend(); }
    vector_criterator<T> iter(GetShmPointer<TypedPointer<vector<T>>>());
    iter.set_begin();
    return iter;
  }

  /** End of the constant reverse iterator */
  static vector_criterator<T> const crend() {
    return vector_criterator<T>::end();
  }
};

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_SHM_DATA_STRUCTURES_LOCKLESS_VECTOR_H_
