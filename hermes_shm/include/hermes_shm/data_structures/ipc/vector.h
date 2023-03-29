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


#ifndef HERMES_DATA_STRUCTURES_LOCKLESS_VECTOR_H_
#define HERMES_DATA_STRUCTURES_LOCKLESS_VECTOR_H_

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"

#include <vector>

namespace hshm::ipc {

/** forward pointer for vector */
template<typename T>
class vector;

/**
 * The vector iterator implementation
 * */
template<typename T, bool FORWARD_ITER>
struct vector_iterator_templ {
 public:
  hipc::Ref<vector<T>> vec_;
  int64_t i_;

  /** Default constructor */
  vector_iterator_templ() = default;

  /** Construct an iterator */
  inline explicit vector_iterator_templ(ShmDeserialize<vector<T>> &&vec)
  : vec_(vec) {}

  /** Construct an iterator at \a i offset */
  inline explicit vector_iterator_templ(const hipc::Ref<vector<T>> &vec,
                                        int64_t i)
  : vec_(vec), i_(i) {}

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
  inline Ref<T> operator*() {
    return Ref<T>(vec_->data_ar()[i_], vec_->GetAllocator());
  }

  /** Dereference the iterator */
  inline const Ref<T> operator*() const {
    return Ref<T>(vec_->data_ar()[i_], vec_->GetAllocator());
  }

  /** Increment iterator in-place */
  inline vector_iterator_templ& operator++() {
    if constexpr(FORWARD_ITER) {
      ++i_;
    } else {
      --i_;
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
    if constexpr(FORWARD_ITER) {
      return vector_iterator_templ(vec_, i_ + count);
    } else {
      return vector_iterator_templ(vec_, i_ - count);
    }
  }

  /** Decrement iterator by \a count and return */
  inline vector_iterator_templ operator-(size_t count) const {
    if constexpr(FORWARD_ITER) {
      return vector_iterator_templ(vec_, i_ - count);
    } else {
      return vector_iterator_templ(vec_, i_ + count);
    }
  }

  /** Increment iterator by \a count in-place */
  inline void operator+=(size_t count) {
    if constexpr(FORWARD_ITER) {
      i_ += count;
    } else {
      i_ -= count;
    }
  }

  /** Decrement iterator by \a count in-place */
  inline void operator-=(size_t count) {
    if constexpr(FORWARD_ITER) {
      i_ -= count;
    } else {
      i_ += count;
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
  inline vector_iterator_templ const end() {
    if constexpr(FORWARD_ITER) {
      return vector_iterator_templ(vec_, vec_->size());
    } else {
      return vector_iterator_templ(vec_, -1);
    }
  }

  /** Set this iterator to end */
  inline void set_end() {
    if constexpr(FORWARD_ITER) {
      i_ = (int64_t)vec_->size();
    } else {
      i_ = -1;
    }
  }

  /** Set this iterator to begin */
  inline void set_begin() {
    if constexpr(FORWARD_ITER) {
      i_ = 0;
    } else {
      i_ = (size_t)(vec_->size() - 1);
    }
  }

  /** Determine whether this iterator is the begin iterator */
  inline bool is_begin() const {
    if constexpr(FORWARD_ITER) {
      return (i_ == 0);
    } else {
      return ((size_t)i_ == vec_->size() - 1);
    }
  }

  /** Determine whether this iterator is the end iterator */
  inline bool is_end() const {
    if constexpr(FORWARD_ITER) {
      return (size_t)i_ == vec_->size();
    } else {
      return i_ == -1;
    }
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
struct ShmHeader<TYPED_CLASS> {
  SHM_CONTAINER_HEADER_TEMPLATE(ShmHeader)
  AtomicPointer vec_ptr_;
  size_t max_length_, length_;

  /** Strong copy operation */
  void strong_copy(const ShmHeader &other) {
    vec_ptr_ = other.vec_ptr_;
    max_length_ = other.max_length_;
    length_ = other.length_;
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
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit vector(TYPED_HEADER *header, Allocator *alloc) {
    shm_init_header(header, alloc);
    SetNull();
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** Construct the vector in shared memory */
  template<typename ...Args>
  explicit vector(TYPED_HEADER *header, Allocator *alloc,
                  size_t length, Args&& ...args) {
    shm_init_header(header, alloc);
    SetNull();
    resize(length, std::forward<Args>(args)...);
  }

  /** SHM copy constructor. From vector. */
  explicit vector(TYPED_HEADER *header, Allocator *alloc,
                  const vector &other) {
    shm_init_header(header, alloc);
    SetNull();
    shm_strong_copy_main<vector<T>>(other);
  }

  /** SHM copy assignment operator. From vector. */
  vector& operator=(const vector &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_main<vector>(other);
    }
    return *this;
  }

  /** SHM copy constructor. From std::vector */
  explicit vector(TYPED_HEADER *header, Allocator *alloc,
                  const std::vector<T> &other) {
    shm_init_header(header, alloc);
    SetNull();
    shm_strong_copy_main<std::vector<T>>(other);
  }

  /** SHM copy assignment operator. From std::vector */
  vector& operator=(const std::vector<T> &other) {
    shm_destroy();
    shm_strong_copy_main<std::vector<T>>(other);
    return *this;
  }

  /** The main copy operation  */
  template<typename VectorT>
  void shm_strong_copy_main(const VectorT &other) {
    reserve(other.size());
    if constexpr(std::is_pod<T>() && !IS_SHM_ARCHIVEABLE(T)) {
      memcpy(data(), other.data(),
             other.size() * sizeof(T));
      header_->length_ = other.size();
    } else {
      for (auto iter = other.cbegin(); iter != other.cend(); ++iter) {
        if constexpr(IS_SHM_ARCHIVEABLE(VectorT)) {
          emplace_back((**iter));
        } else {
          emplace_back((*iter));
        }
      }
    }
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  vector(TYPED_HEADER *header, Allocator *alloc, vector &&other) {
    shm_init_header(header, alloc);
    if (alloc_ == other.alloc_) {
      // memcpy((void*)header_, (void*)other.header_, sizeof(*header_));
      (*header_) = (*other.header_);
      other.SetNull();
    } else {
      shm_strong_copy_main<vector>(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. */
  vector& operator=(vector &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (alloc_ == other.alloc_) {
        (*header_) = (*other.header_);
        other.SetNull();
      } else {
        shm_strong_copy_main<vector>(other);
        other.shm_destroy();
      }
    }
    return *this;
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** Check if null */
  bool IsNull() {
    return header_->vec_ptr_.IsNull();
  }

  /** Make null */
  void SetNull() {
    header_->length_ = 0;
    header_->max_length_ = 0;
    header_->vec_ptr_.SetNull();
  }

  /** Destroy all shared memory allocated by the vector */
  void shm_destroy_main() {
    erase(begin(), end());
    alloc_->Free(header_->vec_ptr_);
  }

  /**====================================
   * SHM Deserialization
   * ===================================*/

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /**====================================
   * Vector Operations
   * ===================================*/

  /**
   * Convert to std::vector
   * */
  std::vector<T> vec() {
    std::vector<T> v;
    v.reserve(size());
    for (hipc::Ref<T> entry : *this) {
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
    if (length == 0) { return; }
    grow_vector(data_ar(), length, true, std::forward<Args>(args)...);
    header_->length_ = length;
  }

  /** Index the vector at position i */
  hipc::Ref<T> operator[](const size_t i) {
    ShmArchive<T> *vec = data_ar();
    return hipc::Ref<T>(vec[i], alloc_);
  }

  /** Get first element of vector */
  hipc::Ref<T> front() {
    return (*this)[0];
  }

  /** Get last element of vector */
  hipc::Ref<T> back() {
    return (*this)[size() - 1];
  }

  /** Index the vector at position i */
  const hipc::Ref<T> operator[](const size_t i) const {
    ShmArchive<T> *vec = data_ar();
    return hipc::Ref<T>(vec[i], alloc_);
  }

  /** Construct an element at the back of the vector */
  template<typename... Args>
  void emplace_back(Args&& ...args) {
    ShmArchive<T> *vec = data_ar();
    if (header_->length_ == header_->max_length_) {
      vec = grow_vector(vec, 0, false);
    }
    make_ref<T>(vec[header_->length_], alloc_, std::forward<Args>(args)...);
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
    ShmArchive<T> *vec = data_ar();
    if (header_->length_ == header_->max_length_) {
      vec = grow_vector(vec, 0, false);
    }
    shift_right(pos);
    make_ref<T>(vec[pos.i_], alloc_, std::forward<Args>(args)...);
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
  void* data() const {
    if (header_ == nullptr) {
      return nullptr;
    }
    return alloc_->template
      Convert<void>(header_->vec_ptr_);
  }

  /**
   * Retreives a pointer to the array from the process-independent pointer.
   * */
  ShmArchive<T>* data_ar() {
    return alloc_->template
      Convert<ShmArchive<T>>(header_->vec_ptr_);
  }

  /**
   * Retreives a pointer to the array from the process-independent pointer.
   * */
  ShmArchive<T>* data_ar() const {
    return alloc_->template
      Convert<ShmArchive<T>>(header_->vec_ptr_);
  }

  /**====================================
   * Internal Operations
   * ===================================*/
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
  ShmArchive<T>* grow_vector(ShmArchive<T> *vec, size_t max_length,
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
    ShmArchive<T> *new_vec;
    if constexpr(std::is_pod<T>() && !IS_SHM_ARCHIVEABLE(T)) {
      // Use reallocate for well-behaved objects
      new_vec = alloc_->template
        ReallocateObjs<ShmArchive<T>>(header_->vec_ptr_, max_length);
    } else {
      // Use std::move for unpredictable objects
      Pointer new_p;
      new_vec = alloc_->template
        AllocateObjs<ShmArchive<T>>(max_length, new_p);
      for (size_t i = 0; i < header_->length_; ++i) {
        hipc::Ref<T> old_entry = (*this)[i];
        hipc::Ref<T> new_entry = make_ref<T>(new_vec[i], alloc_,
                                             std::move(*old_entry));
      }
      if (!header_->vec_ptr_.IsNull()) {
        alloc_->Free(header_->vec_ptr_);
      }
      header_->vec_ptr_ = new_p;
    }
    if (new_vec == nullptr) {
      throw OUT_OF_MEMORY.format("vector::emplace_back",
                                 max_length*sizeof(ShmArchive<T>));
    }
    if (resize) {
      for (size_t i = header_->length_; i < max_length; ++i) {
        hipc::make_ref<T>(new_vec[i], alloc_, std::forward<Args>(args)...);
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
    ShmArchive<T> *vec = data_ar();
    for (int i = 0; i < count; ++i) {
      hipc::Ref<T>(vec[pos.i_ + i], alloc_).shm_destroy();
    }
    auto dst = vec + pos.i_;
    auto src = dst + count;
    for (auto i = pos.i_ + count; i < (int64_t)size(); ++i) {
      memcpy((void*)dst, (void*)src, sizeof(ShmArchive<T>));
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
    auto sz = static_cast<int64_t>(size());
    for (auto i = sz - 1; i >= pos.i_; --i) {
      memcpy((void*)dst, (void*)src, sizeof(ShmArchive<T>));
      dst -= 1; src -= 1;
    }
  }

  /**====================================
   * Iterators
   * ===================================*/
 public:
  /** Beginning of the forward iterator */
  vector_iterator<T> begin() {
    vector_iterator<T> iter(ShmDeserialize<vector<T>>(header_, alloc_));
    iter.set_begin();
    return iter;
  }

  /** End of the forward iterator */
  vector_iterator<T> const end() {
    vector_iterator<T> iter(ShmDeserialize<vector<T>>(header_, alloc_));
    iter.set_end();
    return iter;
  }

  /** Beginning of the constant forward iterator */
  vector_citerator<T> cbegin() const {
    vector_citerator<T> iter(ShmDeserialize<vector<T>>(header_, alloc_));
    iter.set_begin();
    return iter;
  }

  /** End of the forward iterator */
  vector_citerator<T> cend() const {
    vector_citerator<T> iter(ShmDeserialize<vector<T>>(header_, alloc_));
    iter.set_end();
    return iter;
  }

  /** Beginning of the reverse iterator */
  vector_riterator<T> rbegin() {
    vector_riterator<T> iter(ShmDeserialize<vector<T>>(header_, alloc_));
    iter.set_begin();
    return iter;
  }

  /** End of the reverse iterator */
  vector_riterator<T> rend() {
    vector_citerator<T> iter(ShmDeserialize<vector<T>>(header_, alloc_));
    iter.set_end();
    return iter;
  }

  /** Beginning of the constant reverse iterator */
  vector_criterator<T> crbegin() const {
    vector_criterator<T> iter(ShmDeserialize(header_, alloc_));
    iter.set_begin();
    return iter;
  }

  /** End of the constant reverse iterator */
  vector_criterator<T> crend() const {
    vector_criterator<T> iter(ShmDeserialize(header_, alloc_));
    iter.set_end();
    return iter;
  }
};

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_DATA_STRUCTURES_LOCKLESS_VECTOR_H_
