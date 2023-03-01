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

#include "hermes_shm/data_structures/internal/shm_internal.h"

namespace hermes_shm::ipc {

/** forward pointer for slist */
template<typename T>
class slist;

/** represents an object within a slist */
template<typename T>
struct slist_entry {
 public:
  OffsetPointer next_ptr_;
  ShmArchiveOrT<T> data_;

  /** Constructor */
  template<typename ...Args>
  explicit slist_entry(Allocator *alloc, Args&& ...args) {
    data_.shm_init(alloc, std::forward<Args>(args)...);
  }

  /** Destructor */
  void shm_destroy(Allocator *alloc) {
    data_.shm_destroy(alloc);
  }

  /** Returns the element stored in the slist */
  ShmRef<T> internal_ref(Allocator *alloc) {
    return ShmRef<T>(data_.internal_ref(alloc));
  }

  /** Returns the element stored in the slist */
  ShmRef<T> internal_ref(Allocator *alloc) const {
    return ShmRef<T>(data_.internal_ref(alloc));
  }
};

/**
 * The slist iterator
 * */
template<typename T>
struct slist_iterator_templ {
 public:
  /**< A shm reference to the containing slist object. */
  hipc::ShmRef<slist<T>> slist_;
  /**< A pointer to the entry in shared memory */
  slist_entry<T> *entry_;
  /**< The offset of the entry in the shared-memory allocator */
  OffsetPointer entry_ptr_;

  /** Default constructor */
  slist_iterator_templ() = default;

  /** End iterator */
  explicit slist_iterator_templ(bool)
  : entry_(nullptr), entry_ptr_(OffsetPointer::GetNull()) {}

  /** Construct an iterator */
  explicit slist_iterator_templ(ShmDeserialize<slist<T>> slist,
                               slist_entry<T> *entry,
                               OffsetPointer entry_ptr)
    : slist_(slist), entry_(entry), entry_ptr_(entry_ptr) {}

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
  ShmRef<T> operator*() {
    return entry_->internal_ref(slist_->GetAllocator());
  }

  /** Get the object the iterator points to */
  const ShmRef<T> operator*() const {
    return entry_->internal_ref();
  }

  /** Get the next iterator (in place) */
  slist_iterator_templ& operator++() {
    if (is_end()) { return *this; }
    entry_ptr_ = entry_->next_ptr_;
    entry_ = slist_->alloc_->template
      Convert<slist_entry<T>>(entry_->next_ptr_);
    return *this;
  }

  /** Get the prior iterator (in place) */
  slist_iterator_templ& operator--() {
    if (is_end() || is_begin()) { return *this; }
    entry_ptr_ = entry_->prior_ptr_;
    entry_ = slist_->alloc_->template
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

  /** Create the end iterator */
  static slist_iterator_templ const end() {
    static const slist_iterator_templ end_iter(true);
    return end_iter;
  }

  /** Determine whether this iterator is the end iterator */
  bool is_end() const {
    return entry_ == nullptr;
  }

  /** Determine whether this iterator is the begin iterator */
  bool is_begin() const {
    if (entry_) {
      return entry_ptr_ == slist_->header_->head_ptr_;
    } else {
      return false;
    }
  }
};

/** forward iterator typedef */
template<typename T>
using slist_iterator = slist_iterator_templ<T>;

/** const forward iterator typedef */
template<typename T>
using slist_citerator = slist_iterator_templ<T>;


/**
 * MACROS used to simplify the slist namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME slist
#define TYPED_CLASS slist<T>
#define TYPED_HEADER ShmHeader<slist<T>>

/**
 * The slist shared-memory header
 * */
template<typename T>
struct ShmHeader<slist<T>> : public ShmBaseHeader {
  OffsetPointer head_ptr_, tail_ptr_;
  size_t length_;

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
    head_ptr_ = other.head_ptr_;
    tail_ptr_ = other.tail_ptr_;
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
 * Doubly linked slist implementation
 * */
template<typename T>
class slist : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))

 public:
  ////////////////////////////
  /// SHM Overrides
  ////////////////////////////

  /** Default constructor */
  slist() = default;

  /** Initialize slist in shared memory */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    header_->length_ = 0;
    header_->head_ptr_.SetNull();
    header_->tail_ptr_.SetNull();
  }

  /** Destroy all shared memory allocated by the slist */
  void shm_destroy_main() {
    clear();
  }

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          Allocator *alloc, slist &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    *header_ = *(other.header_);
  }

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            Allocator *alloc, const slist &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    for (auto iter = other.cbegin(); iter != other.cend(); ++iter) {
      emplace_back(**iter);
    }
  }

  ////////////////////////////
  /// slist Methods
  ////////////////////////////

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
  void emplace(slist_iterator<T> pos, Args&&... args) {
    OffsetPointer entry_ptr;
    auto entry = _create_entry(entry_ptr, std::forward<Args>(args)...);
    if (size() == 0) {
      entry->next_ptr_.SetNull();
      header_->head_ptr_ = entry_ptr;
      header_->tail_ptr_ = entry_ptr;
    } else if (pos.is_begin()) {
      entry->next_ptr_ = header_->head_ptr_;
      header_->head_ptr_ = entry_ptr;
    } else if (pos.is_end()) {
      entry->next_ptr_.SetNull();
      auto tail = alloc_->template
        Convert<slist_entry<T>>(header_->tail_ptr_);
      tail->next_ptr_ = entry_ptr;
      header_->tail_ptr_ = entry_ptr;
    } else {
      auto prior_iter = find_prior(pos);
      slist_entry<T> *prior = prior_iter.entry_;
      entry->next_ptr_ = pos.entry_->next_ptr_;
      prior->next_ptr_ = entry_ptr;
    }
    ++header_->length_;
  }

  /** Find the element prior to an slist_entry */
  slist_iterator<T> find_prior(slist_iterator<T> pos) {
    if (pos.is_end()) {
      return last();
    } else if (pos.is_begin()) {
      return end();
    } else {
      slist_iterator<T> prior_iter = end();
      for (auto iter = begin(); !iter.is_end(); ++iter) {
        hipc::ShmRef<T> data = *iter;
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
  void erase(slist_iterator<T> pos) {
    erase(pos, pos+1);
  }

  /** Erase all elements between first and last */
  void erase(slist_iterator<T> first,
             slist_iterator<T> last) {
    if (first.is_end()) { return; }
    auto first_prior = find_prior(first);
    auto pos = first;
    while (pos != last) {
      auto next = pos + 1;
      pos.entry_->shm_destroy(alloc_);
      Allocator::DestructObj<slist_entry<T>>(*pos.entry_);
      alloc_->Free(pos.entry_ptr_);
      --header_->length_;
      pos = next;
    }

    if (first_prior.is_end()) {
      header_->head_ptr_ = last.entry_ptr_;
    } else {
      first_prior.entry_->next_ptr_ = last.entry_ptr_;
    }

    if (last.entry_ptr_.IsNull()) {
      header_->tail_ptr_ = first_prior.entry_ptr_;
    }
  }

  /** Destroy all elements in the slist */
  void clear() {
    erase(begin(), end());
  }

  /** Get the object at the front of the slist */
  ShmRef<T> front() {
    return *begin();
  }

  /** Get the object at the back of the slist */
  ShmRef<T> back() {
    return *last();
  }

  /** Get the number of elements in the slist */
  size_t size() const {
    if (!IsNull()) {
      return header_->length_;
    }
    return 0;
  }

  /** Find an element in this slist */
  slist_iterator<T> find(const T &entry) {
    for (auto iter = begin(); iter != end(); ++iter) {
      hipc::ShmRef<T> ref = *iter;
      if (*ref == entry) {
        return iter;
      }
    }
    return end();
  }

  /**
   * ITERATORS
   * */

  /** Forward iterator begin */
  slist_iterator<T> begin() {
    if (size() == 0) { return end(); }
    auto head = alloc_->template
      Convert<slist_entry<T>>(header_->head_ptr_);
    return slist_iterator<T>(GetShmDeserialize(),
      head, header_->head_ptr_);
  }

  /** Forward iterator end */
  static slist_iterator<T> const end() {
    return slist_iterator<T>::end();
  }

  /** Forward iterator to last entry of list */
  slist_iterator<T> last() {
    if (size() == 0) { return end(); }
    auto tail = alloc_->template
      Convert<slist_entry<T>>(header_->tail_ptr_);
    return slist_iterator<T>(GetShmDeserialize(),
                             tail, header_->tail_ptr_);
  }

  /** Constant forward iterator begin */
  slist_citerator<T> cbegin() const {
    if (size() == 0) { return cend(); }
    auto head = alloc_->template
      Convert<slist_entry<T>>(header_->head_ptr_);
    return slist_citerator<T>(GetShmDeserialize(),
      head, header_->head_ptr_);
  }

  /** Constant forward iterator end */
  static slist_citerator<T> const cend() {
    return slist_citerator<T>::end();
  }

 private:
  template<typename ...Args>
  slist_entry<T>* _create_entry(OffsetPointer &p, Args&& ...args) {
    auto entry = alloc_->template
      AllocateConstructObjs<slist_entry<T>>(
        1, p, alloc_, std::forward<Args>(args)...);
    return entry;
  }
};

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_DATA_STRUCTURES_THREAD_UNSAFE_Sslist_H
