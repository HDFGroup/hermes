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

#ifndef HERMES_SHM_DATA_STRUCTURES_THREAD_UNSAFE_LIST_H_
#define HERMES_SHM_DATA_STRUCTURES_THREAD_UNSAFE_LIST_H_

#include "hermes_shm/data_structures/data_structure.h"
#include "hermes_shm/data_structures/internal/shm_archive_or_t.h"

#include <list>

namespace hermes_shm::ipc {

/** forward pointer for list */
template<typename T>
class list;

/** represents an object within a list */
template<typename T>
struct list_entry {
 public:
  OffsetPointer next_ptr_, prior_ptr_;
  ShmHeaderOrT<T> data_;

  /** Constructor */
  template<typename ...Args>
  explicit list_entry(Allocator *alloc, Args ...args)
  : data_(alloc, std::forward<Args>(args)...) {}

  /** Destructor */
  void shm_destroy(Allocator *alloc) {
    data_.shm_destroy(alloc);
  }

  /** Returns the element stored in the list */
  ShmRef<T> internal_ref(Allocator *alloc) {
    return ShmRef<T>(data_.internal_ref(alloc));
  }

  /** Returns the element stored in the list */
  ShmRef<T> internal_ref(Allocator *alloc) const {
    return ShmRef<T>(data_.internal_ref(alloc));
  }
};

/**
 * The list iterator
 * */
template<typename T>
struct list_iterator_templ {
 public:
  /**< A shm reference to the containing list object. */
  hipc::ShmRef<list<T>> list_;
  /**< A pointer to the entry in shared memory */
  list_entry<T> *entry_;
  /**< The offset of the entry in the shared-memory allocator */
  OffsetPointer entry_ptr_;

  /** Default constructor */
  list_iterator_templ() = default;

  /** End iterator */
  explicit list_iterator_templ(bool)
  : entry_(nullptr), entry_ptr_(OffsetPointer::GetNull()) {}

  /** Construct an iterator  */
  explicit list_iterator_templ(TypedPointer<list<T>> list)
  : list_(list), entry_(nullptr), entry_ptr_(OffsetPointer::GetNull()) {}

  /** Construct an iterator  */
  explicit list_iterator_templ(TypedPointer<list<T>> list,
                               list_entry<T> *entry,
                               OffsetPointer entry_ptr)
    : list_(list), entry_(entry), entry_ptr_(entry_ptr) {}

  /** Copy constructor */
  list_iterator_templ(const list_iterator_templ &other) {
    list_ = other.list_;
    entry_ = other.entry_;
    entry_ptr_ = other.entry_ptr_;
  }

  /** Assign this iterator from another iterator */
  list_iterator_templ& operator=(const list_iterator_templ &other) {
    if (this != &other) {
      list_ = other.list_;
      entry_ = other.entry_;
      entry_ptr_ = other.entry_ptr_;
    }
    return *this;
  }

  /** Get the object the iterator points to */
  ShmRef<T> operator*() {
    return entry_->internal_ref(list_->GetAllocator());
  }

  /** Get the object the iterator points to */
  const ShmRef<T> operator*() const {
    return entry_->internal_ref();
  }

  /** Get the next iterator (in place) */
  list_iterator_templ& operator++() {
    if (is_end()) { return *this; }
    entry_ptr_ = entry_->next_ptr_;
    entry_ = list_->alloc_->template
      Convert<list_entry<T>>(entry_->next_ptr_);
    return *this;
  }

  /** Get the prior iterator (in place) */
  list_iterator_templ& operator--() {
    if (is_end() || is_begin()) { return *this; }
    entry_ptr_ = entry_->prior_ptr_;
    entry_ = list_->alloc_->template
      Convert<list_entry<T>>(entry_->prior_ptr_);
    return *this;
  }

  /** Return the next iterator */
  list_iterator_templ operator++(int) const {
    list_iterator_templ next_iter(*this);
    ++next_iter;
    return next_iter;
  }

  /** Return the prior iterator */
  list_iterator_templ operator--(int) const {
    list_iterator_templ prior_iter(*this);
    --prior_iter;
    return prior_iter;
  }

  /** Return the iterator at count after this one */
  list_iterator_templ operator+(size_t count) const {
    list_iterator_templ pos(*this);
    for (size_t i = 0; i < count; ++i) {
      ++pos;
    }
    return pos;
  }

  /** Return the iterator at count before this one */
  list_iterator_templ operator-(size_t count) const {
    list_iterator_templ pos(*this);
    for (size_t i = 0; i < count; ++i) {
      --pos;
    }
    return pos;
  }

  /** Get the iterator at count after this one (in-place) */
  void operator+=(size_t count) {
    list_iterator_templ pos = (*this) + count;
    entry_ = pos.entry_;
    entry_ptr_ = pos.entry_ptr_;
  }

  /** Get the iterator at count before this one (in-place) */
  void operator-=(size_t count) {
    list_iterator_templ pos = (*this) - count;
    entry_ = pos.entry_;
    entry_ptr_ = pos.entry_ptr_;
  }

  /** Determine if two iterators are equal */
  friend bool operator==(const list_iterator_templ &a,
                         const list_iterator_templ &b) {
    return (a.is_end() && b.is_end()) || (a.entry_ == b.entry_);
  }

  /** Determine if two iterators are inequal */
  friend bool operator!=(const list_iterator_templ &a,
                         const list_iterator_templ &b) {
    return !(a.is_end() && b.is_end()) && (a.entry_ != b.entry_);
  }

  /** Create the end iterator */
  static list_iterator_templ const end() {
    const static list_iterator_templ end_iter(true);
    return end_iter;
  }

  /** Determine whether this iterator is the end iterator */
  bool is_end() const {
    return entry_ == nullptr;
  }

  /** Determine whether this iterator is the begin iterator */
  bool is_begin() const {
    if (entry_) {
      return entry_->prior_ptr_.IsNull();
    } else {
      return false;
    }
  }
};

/** forward iterator typedef */
template<typename T>
using list_iterator = list_iterator_templ<T>;

/** const forward iterator typedef */
template<typename T>
using list_citerator = list_iterator_templ<T>;


/**
 * MACROS used to simplify the list namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */
#define CLASS_NAME list
#define TYPED_CLASS list<T>
#define TYPED_HEADER ShmHeader<list<T>>

/**
 * The list shared-memory header
 * */
template<typename T>
struct ShmHeader<list<T>> : public ShmBaseHeader {
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
 * Doubly linked list implementation
 * */
template<typename T>
class list : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))

 public:
  ////////////////////////////
  /// SHM Overrides
  ////////////////////////////

  /** Default constructor */
  list() = default;

  /** Initialize list in shared memory */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    header_->length_ = 0;
    header_->head_ptr_.SetNull();
    header_->tail_ptr_.SetNull();
  }

  /** Copy from std::list */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc, std::list<T> &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    for (auto &entry : other) {
      emplace_back(entry);
    }
  }

  /** Destroy all shared memory allocated by the list */
  void shm_destroy_main() {
    clear();
  }

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          Allocator *alloc, list &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    *header_ = *(other.header_);
  }

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            Allocator *alloc, const list &other) {
    shm_init_allocator(alloc);
    shm_init_header(header);
    for (auto iter = other.cbegin(); iter != other.cend(); ++iter) {
      emplace_back(**iter);
    }
  }

  ////////////////////////////
  /// List Methods
  ////////////////////////////

  /** Construct an element at the back of the list */
  template<typename... Args>
  void emplace_back(Args&&... args) {
    emplace(end(), std::forward<Args>(args)...);
  }

  /** Construct an element at the beginning of the list */
  template<typename... Args>
  void emplace_front(Args&&... args) {
    emplace(begin(), std::forward<Args>(args)...);
  }

  /** Construct an element at \a pos position in the list */
  template<typename ...Args>
  void emplace(list_iterator<T> pos, Args&&... args) {
    OffsetPointer entry_ptr;
    auto entry = _create_entry(entry_ptr, std::forward<Args>(args)...);
    if (size() == 0) {
      entry->prior_ptr_.SetNull();
      entry->next_ptr_.SetNull();
      header_->head_ptr_ = entry_ptr;
      header_->tail_ptr_ = entry_ptr;
    } else if (pos.is_begin()) {
      entry->prior_ptr_.SetNull();
      entry->next_ptr_ = header_->head_ptr_;
      auto head = alloc_->template
        Convert<list_entry<T>>(header_->tail_ptr_);
      head->prior_ptr_ = entry_ptr;
      header_->head_ptr_ = entry_ptr;
    } else if (pos.is_end()) {
      entry->prior_ptr_ = header_->tail_ptr_;
      entry->next_ptr_.SetNull();
      auto tail = alloc_->template
        Convert<list_entry<T>>(header_->tail_ptr_);
      tail->next_ptr_ = entry_ptr;
      header_->tail_ptr_ = entry_ptr;
    } else {
      auto next = alloc_->template
        Convert<list_entry<T>>(pos.entry_->next_ptr_);
      auto prior = alloc_->template
        Convert<list_entry<T>>(pos.entry_->prior_ptr_);
      entry->next_ptr_ = pos.entry_->next_ptr_;
      entry->prior_ptr_ = pos.entry_->prior_ptr_;
      next->prior_ptr_ = entry_ptr;
      prior->next_ptr_ = entry_ptr;
    }
    ++header_->length_;
  }

  /** Erase element with ID */
  void erase(const T &entry) {
    auto iter = find(entry);
    erase(iter);
  }

  /** Erase the element at pos */
  void erase(list_iterator<T> pos) {
    erase(pos, pos+1);
  }

  /** Erase all elements between first and last */
  void erase(list_iterator<T> first,
             list_iterator<T> last) {
    if (first.is_end()) { return; }
    auto first_prior_ptr = first.entry_->prior_ptr_;
    auto pos = first;
    while (pos != last) {
      auto next = pos + 1;
      pos.entry_->shm_destroy(alloc_);
      Allocator::DestructObj<list_entry<T>>(*pos.entry_);
      alloc_->Free(pos.entry_ptr_);
      --header_->length_;
      pos = next;
    }

    if (first_prior_ptr.IsNull()) {
      header_->head_ptr_ = last.entry_ptr_;
    } else {
      auto first_prior = alloc_->template
        Convert<list_entry<T>>(first_prior_ptr);
      first_prior->next_ptr_ = last.entry_ptr_;
    }

    if (last.entry_ptr_.IsNull()) {
      header_->tail_ptr_ = first_prior_ptr;
    } else {
      last.entry_->prior_ptr_ = first_prior_ptr;
    }
  }

  /** Destroy all elements in the list */
  void clear() {
    erase(begin(), end());
  }

  /** Get the object at the front of the list */
  ShmRef<T> front() {
    return *begin();
  }

  /** Get the object at the back of the list */
  ShmRef<T> back() {
    return *end();
  }

  /** Get the number of elements in the list */
  size_t size() const {
    if (!IsNull()) {
      return header_->length_;
    }
    return 0;
  }
  
  /** Find an element in this list */
  list_iterator<T> find(const T &entry) {
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
  list_iterator<T> begin() {
    if (size() == 0) { return end(); }
    auto head = alloc_->template
      Convert<list_entry<T>>(header_->head_ptr_);
    return list_iterator<T>(GetShmPointer<TypedPointer<list<T>>>(),
      head, header_->head_ptr_);
  }

  /** Forward iterator end */
  static list_iterator<T> const end() {
    return list_iterator<T>::end();
  }

  /** Constant forward iterator begin */
  list_citerator<T> cbegin() const {
    if (size() == 0) { return cend(); }
    auto head = alloc_->template
      Convert<list_entry<T>>(header_->head_ptr_);
    return list_citerator<T>(GetShmPointer<TypedPointer<list<T>>>(),
      head, header_->head_ptr_);
  }

  /** Constant forward iterator end */
  static list_citerator<T> const cend() {
    return list_citerator<T>::end();
  }

 private:
  template<typename ...Args>
  list_entry<T>* _create_entry(OffsetPointer &p, Args&& ...args) {
    auto entry = alloc_->template
      AllocateConstructObjs<list_entry<T>>(
        1, p, alloc_, std::forward<Args>(args)...);
    return entry;
  }
};

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_SHM_DATA_STRUCTURES_THREAD_UNSAFE_LIST_H_
