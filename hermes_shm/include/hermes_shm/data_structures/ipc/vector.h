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
  vector<T> *vec_;
  off64_t i_;

  /** Default constructor */
  HSHM_ALWAYS_INLINE vector_iterator_templ() = default;

  /** Construct an iterator (called from vector class) */
  template<typename SizeT>
  HSHM_ALWAYS_INLINE explicit vector_iterator_templ(vector<T> *vec, SizeT i)
  : vec_(vec), i_(static_cast<off64_t>(i)) {}

  /** Construct an iterator (called from iterator) */
  HSHM_ALWAYS_INLINE explicit vector_iterator_templ(vector<T> *vec, off64_t i)
  : vec_(vec), i_(i) {}

  /** Copy constructor */
  HSHM_ALWAYS_INLINE vector_iterator_templ(const vector_iterator_templ &other)
  : vec_(other.vec_), i_(other.i_) {}

  /** Copy assignment operator  */
  HSHM_ALWAYS_INLINE vector_iterator_templ&
  operator=(const vector_iterator_templ &other) {
    if (this != &other) {
      vec_ = other.vec_;
      i_ = other.i_;
    }
    return *this;
  }

  /** Move constructor */
  HSHM_ALWAYS_INLINE vector_iterator_templ(
    vector_iterator_templ &&other) noexcept {
    vec_ = other.vec_;
    i_ = other.i_;
  }

  /** Move assignment operator  */
  HSHM_ALWAYS_INLINE vector_iterator_templ&
  operator=(vector_iterator_templ &&other) noexcept {
    if (this != &other) {
      vec_ = other.vec_;
      i_ = other.i_;
    }
    return *this;
  }

  /** Dereference the iterator */
  HSHM_ALWAYS_INLINE T& operator*() {
    return vec_->data_ar()[i_].get_ref();
  }

  /** Dereference the iterator */
  HSHM_ALWAYS_INLINE const T& operator*() const {
    return vec_->data_ar()[i_].get_ref();
  }

  /** Increment iterator in-place */
  HSHM_ALWAYS_INLINE vector_iterator_templ& operator++() {
    if constexpr(FORWARD_ITER) {
      ++i_;
    } else {
      --i_;
    }
    return *this;
  }

  /** Decrement iterator in-place */
  HSHM_ALWAYS_INLINE vector_iterator_templ& operator--() {
    if constexpr(FORWARD_ITER) {
      --i_;
    } else {
      ++i_;
    }
    return *this;
  }

  /** Create the next iterator */
  HSHM_ALWAYS_INLINE vector_iterator_templ operator++(int) const {
    vector_iterator_templ next_iter(*this);
    ++next_iter;
    return next_iter;
  }

  /** Create the prior iterator */
  HSHM_ALWAYS_INLINE vector_iterator_templ operator--(int) const {
    vector_iterator_templ prior_iter(*this);
    --prior_iter;
    return prior_iter;
  }

  /** Increment iterator by \a count and return */
  HSHM_ALWAYS_INLINE vector_iterator_templ operator+(size_t count) const {
    if constexpr(FORWARD_ITER) {
      return vector_iterator_templ(vec_, i_ + count);
    } else {
      return vector_iterator_templ(vec_, i_ - count);
    }
  }

  /** Decrement iterator by \a count and return */
  HSHM_ALWAYS_INLINE vector_iterator_templ operator-(size_t count) const {
    if constexpr(FORWARD_ITER) {
      return vector_iterator_templ(vec_, i_ - count);
    } else {
      return vector_iterator_templ(vec_, i_ + count);
    }
  }

  /** Increment iterator by \a count in-place */
  HSHM_ALWAYS_INLINE void operator+=(size_t count) {
    if constexpr(FORWARD_ITER) {
      i_ += count;
    } else {
      i_ -= count;
    }
  }

  /** Decrement iterator by \a count in-place */
  HSHM_ALWAYS_INLINE void operator-=(size_t count) {
    if constexpr(FORWARD_ITER) {
      i_ -= count;
    } else {
      i_ += count;
    }
  }

  /** Check if two iterators are equal */
  HSHM_ALWAYS_INLINE friend bool operator==(const vector_iterator_templ &a,
                         const vector_iterator_templ &b) {
    return (a.i_ == b.i_);
  }

  /** Check if two iterators are inequal */
  HSHM_ALWAYS_INLINE friend bool operator!=(const vector_iterator_templ &a,
                         const vector_iterator_templ &b) {
    return (a.i_ != b.i_);
  }

  /** Set this iterator to end */
  HSHM_ALWAYS_INLINE void set_end() {
    if constexpr(FORWARD_ITER) {
      i_ = vec_->size();
    } else {
      i_ = -1;
    }
  }

  /** Set this iterator to begin */
  HSHM_ALWAYS_INLINE void set_begin() {
    if constexpr(FORWARD_ITER) {
      i_ = 0;
    } else {
      i_ = vec_->size() - 1;
    }
  }

  /** Determine whether this iterator is the begin iterator */
  HSHM_ALWAYS_INLINE bool is_begin() const {
    if constexpr(FORWARD_ITER) {
      return (i_ == 0);
    } else {
      return (i_ == vec_->template size<off64_t>() - 1);
    }
  }

  /** Determine whether this iterator is the end iterator */
  HSHM_ALWAYS_INLINE bool is_end() const {
    if constexpr(FORWARD_ITER) {
      return i_ >= vec_->template size<off64_t>();
    } else {
      return i_ == -1;
    }
  }
};

/**
 * MACROS used to simplify the vector namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME vector
#define TYPED_CLASS vector<T>
#define TYPED_HEADER ShmHeader<vector<T>>

/**
 * The vector class
 * */
template<typename T>
class vector : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS))

 public:
  /**====================================
   * Typedefs
   * ===================================*/

  /** forwrard iterator */
  typedef vector_iterator_templ<T, true>  iterator_t;
  /** reverse iterator */
  typedef vector_iterator_templ<T, false> riterator_t;
  /** const iterator */
  typedef vector_iterator_templ<T, true>  citerator_t;
  /** const reverse iterator */
  typedef vector_iterator_templ<T, false> criterator_t;

 public:
  /**====================================
   * Variables
   * ===================================*/
  AtomicPointer vec_ptr_;
  size_t max_length_, length_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit vector(Allocator *alloc) {
    shm_init_container(alloc);
    SetNull();
  }

  /** SHM constructor. Resize + construct. */
  template<typename ...Args>
  explicit vector(Allocator *alloc, size_t length, Args&& ...args) {
    shm_init_container(alloc);
    SetNull();
    resize(length, std::forward<Args>(args)...);
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor. From vector. */
  explicit vector(Allocator *alloc, const vector &other) {
    shm_init_container(alloc);
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
  explicit vector(Allocator *alloc, const std::vector<T> &other) {
    shm_init_container(alloc);
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
      memcpy(data(), other.data(), other.size() * sizeof(T));
      length_ = other.size();
    } else {
      for (auto iter = other.cbegin(); iter != other.cend(); ++iter) {
        if constexpr(IS_SHM_ARCHIVEABLE(VectorT)) {
          emplace_back((*iter));
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
  vector(Allocator *alloc, vector &&other) {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      memcpy((void *) this, (void *) &other, sizeof(*this));
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
      if (GetAllocator() == other.GetAllocator()) {
        memcpy((void *) this, (void *) &other, sizeof(*this));
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
  HSHM_ALWAYS_INLINE bool IsNull() const {
    return vec_ptr_.IsNull();
  }

  /** Make null */
  HSHM_ALWAYS_INLINE void SetNull() {
    length_ = 0;
    max_length_ = 0;
    vec_ptr_.SetNull();
  }

  /** Destroy all shared memory allocated by the vector */
  HSHM_ALWAYS_INLINE void shm_destroy_main() {
    erase(begin(), end());
    GetAllocator()->Free(vec_ptr_);
  }

  /**====================================
   * Vector Operations
   * ===================================*/

  /**
   * Convert to std::vector
   * */
  HSHM_ALWAYS_INLINE std::vector<T> vec() {
    std::vector<T> v;
    v.reserve(size());
    for (T& entry : *this) {
      v.emplace_back(entry);
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
  HSHM_ALWAYS_INLINE void reserve(size_t length, Args&& ...args) {
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
    if (length == 0) {
      length_ = 0;
      return;
    }
    grow_vector(data_ar(), length, true,
                std::forward<Args>(args)...);
    length_ = length;
  }

  /** Index the vector at position i */
  HSHM_ALWAYS_INLINE T& operator[](const size_t i) {
      return data_ar()[i].get_ref();
  }

  /** Index the vector at position i */
  HSHM_ALWAYS_INLINE const T& operator[](const size_t i) const {
    return data_ar()[i].get_ref();
  }

  /** Get first element of vector */
  HSHM_ALWAYS_INLINE T& front() {
    return (*this)[0];
  }

  /** Get last element of vector */
  HSHM_ALWAYS_INLINE T& back() {
    return (*this)[size() - 1];
  }

  /** Construct an element at the back of the vector */
  template<typename... Args>
  void emplace_back(Args&& ...args) {
    ShmArchive<T> *vec = data_ar();
    if (length_ == max_length_) {
      vec = grow_vector(vec, 0, false);
    }
    HSHM_MAKE_AR(vec[length_], GetAllocator(),
                 std::forward<Args>(args)...)
    ++length_;
  }

  /** Construct an element in the front of the vector */
  template<typename ...Args>
  HSHM_ALWAYS_INLINE void emplace_front(Args&& ...args) {
    emplace(begin(), std::forward<Args>(args)...);
  }

  /** Construct an element at an arbitrary position in the vector */
  template<typename ...Args>
  void emplace(iterator_t pos, Args&&... args) {
    if (pos.is_end()) {
      emplace_back(std::forward<Args>(args)...);
      return;
    }
    ShmArchive<T> *vec = data_ar();
    if (length_ == max_length_) {
      vec = grow_vector(vec, 0, false);
    }
    shift_right(pos);
    HSHM_MAKE_AR(vec[pos.i_], GetAllocator(),
                 std::forward<Args>(args)...)
    ++length_;
  }

  /** Replace an element at a position */
  template<typename ...Args>
  HSHM_ALWAYS_INLINE void replace(iterator_t pos, Args&&... args) {
    if (pos.is_end()) {
      return;
    }
    ShmArchive<T> *vec = data_ar();
    hipc::Allocator::DestructObj((*this)[pos.i_]);
    HSHM_MAKE_AR(vec[pos.i_], GetAllocator(),
                 std::forward<Args>(args)...)
  }

  /** Delete the element at \a pos position */
  HSHM_ALWAYS_INLINE void erase(iterator_t pos) {
    if (pos.is_end()) return;
    shift_left(pos, 1);
    length_ -= 1;
  }

  /** Delete elements between first and last  */
  HSHM_ALWAYS_INLINE void erase(iterator_t first, iterator_t last) {
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
    length_ -= count;
  }

  /** Delete all elements from the vector */
  HSHM_ALWAYS_INLINE void clear() {
    erase(begin(), end());
  }

  /** Get the size of the vector */
  template<typename SizeT = size_t>
  HSHM_ALWAYS_INLINE SizeT size() const {
    return static_cast<SizeT>(length_);
  }

  /** Get the data in the vector */
  HSHM_ALWAYS_INLINE void* data() {
    return reinterpret_cast<void*>(data_ar());
  }

  /** Get constant pointer to the data */
  HSHM_ALWAYS_INLINE void* data() const {
    return reinterpret_cast<void*>(data_ar());
  }

  /** Retreives a pointer to the internal array */
  HSHM_ALWAYS_INLINE ShmArchive<T>* data_ar() {
    return GetAllocator()->template Convert<ShmArchive<T>>(vec_ptr_);
  }

  /** Retreives a pointer to the array */
  HSHM_ALWAYS_INLINE ShmArchive<T>* data_ar() const {
    return GetAllocator()->template Convert<ShmArchive<T>>(vec_ptr_);
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
      max_length = 5 * max_length_ / 4;
      if (max_length <= max_length_ + 10) {
        max_length += 10;
      }
    }
    if (max_length < max_length_) {
      return nullptr;
    }

    // Allocate new shared-memory vec
    ShmArchive<T> *new_vec;
    if constexpr(std::is_pod<T>() && !IS_SHM_ARCHIVEABLE(T)) {
      // Use reallocate for well-behaved objects
      new_vec = GetAllocator()->template
        ReallocateObjs<ShmArchive<T>>(vec_ptr_, max_length);
    } else {
      // Use std::move for unpredictable objects
      Pointer new_p;
      new_vec = GetAllocator()->template
        AllocateObjs<ShmArchive<T>>(max_length, new_p);
      for (size_t i = 0; i < length_; ++i) {
        T& old_entry = (*this)[i];
        HSHM_MAKE_AR(new_vec[i], GetAllocator(),
                     std::move(old_entry))
      }
      if (!vec_ptr_.IsNull()) {
        GetAllocator()->Free(vec_ptr_);
      }
      vec_ptr_ = new_p;
    }
    if (new_vec == nullptr) {
      throw OUT_OF_MEMORY.format("vector::emplace_back",
                                 max_length*sizeof(ShmArchive<T>));
    }
    if (resize) {
      for (size_t i = length_; i < max_length; ++i) {
        HSHM_MAKE_AR(new_vec[i], GetAllocator(),
                     std::forward<Args>(args)...)
      }
    }

    // Update vector header
    max_length_ = max_length;
    return new_vec;
  }

  /**
   * Shift every element starting at "pos" to the left by count. Any element
   * who would be shifted before "pos" will be deleted.
   *
   * @param pos the starting position
   * @param count the amount to shift left by
   * */
  void shift_left(const iterator_t pos, size_t count = 1) {
    ShmArchive<T> *vec = data_ar();
    for (size_t i = 0; i < count; ++i) {
      HSHM_DESTROY_AR(vec[pos.i_ + i])
    }
    auto dst = vec + pos.i_;
    auto src = dst + count;
    for (auto i = pos.i_ + count; i < size(); ++i) {
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
  void shift_right(const iterator_t pos, size_t count = 1) {
    auto src = data_ar() + size() - 1;
    auto dst = src + count;
    auto sz = static_cast<off64_t>(size());
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
  iterator_t begin() {
    return iterator_t(this, 0);
  }

  /** End of the forward iterator */
  iterator_t end() {
    return iterator_t(this, size());
  }

  /** Beginning of the constant forward iterator */
  citerator_t cbegin() const {
    return citerator_t(const_cast<vector*>(this), 0);
  }

  /** End of the forward iterator */
  citerator_t cend() const {
    return citerator_t(const_cast<vector*>(this), size<off64_t>());
  }

  /** Beginning of the reverse iterator */
  riterator_t rbegin() {
    return riterator_t(this, size<off64_t>() - 1);
  }

  /** End of the reverse iterator */
  riterator_t rend() {
    return citerator_t(this, (off64_t)-1);
  }

  /** Beginning of the constant reverse iterator */
  criterator_t crbegin() const {
    return criterator_t(const_cast<vector*>(this), size<off64_t>() - 1);
  }

  /** End of the constant reverse iterator */
  criterator_t crend() const {
    return criterator_t(const_cast<vector*>(this), (off64_t)-1);
  }
};

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_DATA_STRUCTURES_LOCKLESS_VECTOR_H_
