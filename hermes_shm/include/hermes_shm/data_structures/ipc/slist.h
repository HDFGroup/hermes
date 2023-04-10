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


#ifndef HERMES_DATA_STRUCTURES_THREAD_UNSAFE_Sslist_H
#define HERMES_DATA_STRUCTURES_THREAD_UNSAFE_Sslist_H

#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"
#include "hermes_shm/data_structures/containers/functional.h"

namespace hshm::ipc {

/** forward pointer for slist */
template<typename T>
class slist;

/** represents an object within a slist */
template<typename T>
struct slist_entry {
 public:
  OffsetPointer next_ptr_;
  ShmArchive<T> data_;

  /** Constructor */
  template<typename ...Args>
  explicit slist_entry(Allocator *alloc, Args&& ...args) {
    HSHM_MAKE_AR(data_, alloc, std::forward<Args>(args)...)
  }
};

/**
 * The slist iterator
 * */
template<typename T>
struct slist_iterator_templ {
 public:
  /**< A shm reference to the containing slist object. */
  slist<T> *slist_;
  /**< A pointer to the entry in shared memory */
  slist_entry<T> *entry_;
  /**< The offset of the entry in the shared-memory allocator */
  OffsetPointer entry_ptr_;

  /** Default constructor */
  slist_iterator_templ() = default;

  /** Construct an iterator */
  explicit slist_iterator_templ(slist<T>& slist,
                                slist_entry<T> *entry,
                                OffsetPointer entry_ptr)
    : slist_(&slist), entry_(entry), entry_ptr_(entry_ptr) {}

  /** Copy constructor */
  slist_iterator_templ(const slist_iterator_templ &other) {
    slist_ = other.slist_;
    entry_ = other.entry_;
    entry_ptr_ = other.entry_ptr_;
  }

  /** Assign this iterator from another iterator */
  slist_iterator_templ& operator=(const slist_iterator_templ &other) {
    if (this != &other) {
      slist_ = other.slist_;
      entry_ = other.entry_;
      entry_ptr_ = other.entry_ptr_;
    }
    return *this;
  }

  /** Get the object the iterator points to */
  T& operator*() {
    return entry_->data_.get_ref();
  }

  /** Get the object the iterator points to */
  const T& operator*() const {
    return entry_->data_.get_ref();
  }

  /** Get the next iterator (in place) */
  slist_iterator_templ& operator++() {
    if (is_end()) { return *this; }
    entry_ptr_ = entry_->next_ptr_;
    entry_ = slist_->GetAllocator()->template
      Convert<slist_entry<T>>(entry_->next_ptr_);
    return *this;
  }

  /** Get the prior iterator (in place) */
  slist_iterator_templ& operator--() {
    if (is_end() || is_begin()) { return *this; }
    entry_ptr_ = entry_->prior_ptr_;
    entry_ = slist_->GetAllocator()->template
      Convert<slist_entry<T>>(entry_->prior_ptr_);
    return *this;
  }

  /** Return the next iterator */
  slist_iterator_templ operator++(int) const {
    slist_iterator_templ next_iter(*this);
    ++next_iter;
    return next_iter;
  }

  /** Return the prior iterator */
  slist_iterator_templ operator--(int) const {
    slist_iterator_templ prior_iter(*this);
    --prior_iter;
    return prior_iter;
  }

  /** Return the iterator at count after this one */
  slist_iterator_templ operator+(size_t count) const {
    slist_iterator_templ pos(*this);
    for (size_t i = 0; i < count; ++i) {
      ++pos;
    }
    return pos;
  }

  /** Return the iterator at count before this one */
  slist_iterator_templ operator-(size_t count) const {
    slist_iterator_templ pos(*this);
    for (size_t i = 0; i < count; ++i) {
      --pos;
    }
    return pos;
  }

  /** Get the iterator at count after this one (in-place) */
  void operator+=(size_t count) {
    slist_iterator_templ pos = (*this) + count;
    entry_ = pos.entry_;
    entry_ptr_ = pos.entry_ptr_;
  }

  /** Get the iterator at count before this one (in-place) */
  void operator-=(size_t count) {
    slist_iterator_templ pos = (*this) - count;
    entry_ = pos.entry_;
    entry_ptr_ = pos.entry_ptr_;
  }

  /** Determine if two iterators are equal */
  friend bool operator==(const slist_iterator_templ &a,
                         const slist_iterator_templ &b) {
    return (a.is_end() && b.is_end()) || (a.entry_ == b.entry_);
  }

  /** Determine if two iterators are inequal */
  friend bool operator!=(const slist_iterator_templ &a,
                         const slist_iterator_templ &b) {
    return !(a.is_end() && b.is_end()) && (a.entry_ != b.entry_);
  }

  /** Determine whether this iterator is the end iterator */
  bool is_end() const {
    return entry_ == nullptr;
  }

  /** Determine whether this iterator is the begin iterator */
  bool is_begin() const {
    if (entry_) {
      return entry_ptr_ == slist_->head_ptr_;
    } else {
      return false;
    }
  }
};

/**
 * MACROS used to simplify the slist namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME slist
#define TYPED_CLASS slist<T>
#define TYPED_HEADER ShmHeader<slist<T>>

/**
 * Doubly linked slist implementation
 * */
template<typename T>
class slist : public ShmContainer {
 public:
  /**====================================
   * Variables
   * ===================================*/
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS))
  OffsetPointer head_ptr_, tail_ptr_;
  size_t length_;

  /**====================================
   * Iterator Typedefs
   * ===================================*/
  /** forward iterator typedef */
  typedef slist_iterator_templ<T> iterator_t;
  /** const forward iterator typedef */
  typedef slist_iterator_templ<T> citerator_t;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /** SHM constructor. Default. */
  explicit slist(Allocator *alloc) {
    shm_init_container(alloc);
    SetNull();
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** Strong copy operation */
  void strong_copy(const slist &other) {
    head_ptr_ = other.head_ptr_;
    tail_ptr_ = other.tail_ptr_;
    length_ = other.length_;
  }

  /** SHM copy constructor. From slist. */
  explicit slist(Allocator *alloc,
                 const slist &other) {
    shm_init_container(alloc);
    SetNull();
    shm_strong_copy_construct_and_op<slist>(other);
  }

  /** SHM copy assignment operator. From slist. */
  slist& operator=(const slist &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_construct_and_op<slist>(other);
    }
    return *this;
  }

  /** SHM copy constructor. From std::list */
  explicit slist(Allocator *alloc,
                 std::list<T> &other) {
    shm_init_container(alloc);
    SetNull();
    shm_strong_copy_construct_and_op<std::list<T>>(other);
  }

  /** SHM copy assignment operator. From std::list. */
  slist& operator=(const std::list<T> &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_construct_and_op<std::list<T>>(other);
    }
    return *this;
  }

  /** SHM copy constructor + operator main */
  template<typename ListT>
  void shm_strong_copy_construct_and_op(const ListT &other) {
    for (auto iter = other.cbegin(); iter != other.cend(); ++iter) {
      emplace_back(*iter);
    }
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. From slist. */
  slist(Allocator *alloc, slist &&other) noexcept {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      strong_copy(other);
      other.SetNull();
    } else {
      shm_strong_copy_construct_and_op<slist>(other);
      other.shm_destroy();
    }
  }

  /** SHM move assignment operator. From slist. */
  slist& operator=(slist &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (GetAllocator() == other.GetAllocator()) {
        strong_copy(other);
        other.SetNull();
      } else {
        shm_strong_copy_construct_and_op<slist>(other);
        other.shm_destroy();
      }
    }
    return *this;
  }

  /**====================================
  * Destructor
  * ===================================*/

  /** Check if the list is empty */
  bool IsNull() const {
    return length_ == 0;
  }

  /** Sets this list as empty */
  void SetNull() {
    length_ = 0;
    head_ptr_.SetNull();
    tail_ptr_.SetNull();
  }

  /** Destroy all shared memory allocated by the slist */
  void shm_destroy_main() {
    clear();
  }

  /**====================================
   * slist Methods
   * ===================================*/

  /** Construct an element at the back of the slist */
  template<typename... Args>
  void emplace_back(Args&&... args) {
    emplace(end(), std::forward<Args>(args)...);
  }

  /** Construct an element at the beginning of the slist */
  template<typename... Args>
  void emplace_front(Args&&... args) {
    emplace(begin(), std::forward<Args>(args)...);
  }

  /** Construct an element at \a pos position in the slist */
  template<typename ...Args>
  void emplace(iterator_t pos, Args&&... args) {
    OffsetPointer entry_ptr;
    auto entry = _create_entry(entry_ptr, std::forward<Args>(args)...);
    if (size() == 0) {
      entry->next_ptr_.SetNull();
      head_ptr_ = entry_ptr;
      tail_ptr_ = entry_ptr;
    } else if (pos.is_begin()) {
      entry->next_ptr_ = head_ptr_;
      head_ptr_ = entry_ptr;
    } else if (pos.is_end()) {
      entry->next_ptr_.SetNull();
      auto tail = GetAllocator()->template
        Convert<slist_entry<T>>(tail_ptr_);
      tail->next_ptr_ = entry_ptr;
      tail_ptr_ = entry_ptr;
    } else {
      auto prior_iter = find_prior(pos);
      slist_entry<T> *prior = prior_iter.entry_;
      entry->next_ptr_ = pos.entry_->next_ptr_;
      prior->next_ptr_ = entry_ptr;
    }
    ++length_;
  }

  /** Find the element prior to an slist_entry */
  iterator_t find_prior(iterator_t pos) {
    if (pos.is_end()) {
      return last();
    } else if (pos.is_begin()) {
      return end();
    } else {
      iterator_t prior_iter = end();
      for (auto iter = begin(); !iter.is_end(); ++iter) {
        if (iter == pos) {
          return prior_iter;
        }
        prior_iter = iter;
      }
      return prior_iter;
    }
  }

  /** Erase element with ID */
  void erase(const T &entry) {
    auto iter = find(entry);
    erase(iter);
  }

  /** Erase the element at pos */
  void erase(iterator_t pos) {
    erase(pos, pos+1);
  }

  /** Erase all elements between first and last */
  void erase(iterator_t first,
             iterator_t last) {
    if (first.is_end()) { return; }
    auto first_prior = find_prior(first);
    auto pos = first;
    while (pos != last) {
      auto next = pos + 1;
      HSHM_DESTROY_AR(pos.entry_->data_)
      GetAllocator()->Free(pos.entry_ptr_);
      --length_;
      pos = next;
    }

    if (first_prior.is_end()) {
      head_ptr_ = last.entry_ptr_;
    } else {
      first_prior.entry_->next_ptr_ = last.entry_ptr_;
    }

    if (last.entry_ptr_.IsNull()) {
      tail_ptr_ = first_prior.entry_ptr_;
    }
  }

  /** Destroy all elements in the slist */
  void clear() {
    erase(begin(), end());
  }

  /** Get the object at the front of the slist */
  T& front() {
    return *begin();
  }

  /** Get the object at the back of the slist */
  T& back() {
    return *last();
  }

  /** Get the number of elements in the slist */
  size_t size() const {
    if (!IsNull()) {
      return length_;
    }
    return 0;
  }

  /** Find an element in this slist */
  iterator_t find(const T &entry) {
    return hshm::find(begin(), end(), entry);
  }

  /**====================================
  * Iterators
  * ===================================*/

  /** Forward iterator begin */
  iterator_t begin() {
    if (size() == 0) { return end(); }
    auto head = GetAllocator()->template
      Convert<slist_entry<T>>(head_ptr_);
    return iterator_t(*this, head, head_ptr_);
  }

  /** Forward iterator end */
  iterator_t end() {
    return iterator_t(*this,
                      nullptr, OffsetPointer::GetNull());
  }

  /** Forward iterator to last entry of list */
  iterator_t last() {
    if (size() == 0) { return end(); }
    auto tail = GetAllocator()->template
      Convert<slist_entry<T>>(tail_ptr_);
    return iterator_t(*this, tail, tail_ptr_);
  }

  /** Constant forward iterator begin */
  citerator_t cbegin() const {
    if (size() == 0) { return cend(); }
    auto head = GetAllocator()->template
      Convert<slist_entry<T>>(head_ptr_);
    return citerator_t(const_cast<slist&>(*this), head, head_ptr_);
  }

  /** Constant forward iterator end */
  citerator_t cend() const {
    return iterator_t(const_cast<slist&>(*this),
                      nullptr, OffsetPointer::GetNull());
  }

 private:
  template<typename ...Args>
  slist_entry<T>* _create_entry(OffsetPointer &p, Args&& ...args) {
    auto entry = GetAllocator()->template
      AllocateObjs<slist_entry<T>>(1, p);
    HSHM_MAKE_AR(entry->data_, GetAllocator(), std::forward<Args>(args)...)
    return entry;
  }
};

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_DATA_STRUCTURES_THREAD_UNSAFE_Sslist_H
