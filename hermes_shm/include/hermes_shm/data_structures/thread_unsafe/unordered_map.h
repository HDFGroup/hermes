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


#ifndef HERMES_DATA_STRUCTURES_UNORDERED_MAP_H_
#define HERMES_DATA_STRUCTURES_UNORDERED_MAP_H_

#include "hermes_shm/thread/thread_manager.h"
#include "hermes_shm/data_structures/thread_unsafe/vector.h"
#include "hermes_shm/data_structures/thread_unsafe/list.h"
#include "hermes_shm/data_structures/pair.h"
#include "hermes_shm/types/atomic.h"
#include "hermes_shm/data_structures/internal/shm_internal.h"

namespace hermes_shm::ipc {

/** forward pointer for unordered_map */
template<typename Key, typename T, class Hash = std::hash<Key>>
class unordered_map;

/**
 * The unordered map iterator (bucket_iter, list_iter)
 * */
template<typename Key, typename T, class Hash>
struct unordered_map_iterator {
 public:
  using COLLISION_T = hipc::pair<Key, T>;
  using BUCKET_T = hipc::list<COLLISION_T>;

 public:
  hipc::ShmRef<unordered_map<Key, T, Hash>> map_;
  vector_iterator<BUCKET_T> bucket_;
  list_iterator<COLLISION_T> collision_;

  /** Default constructor */
  unordered_map_iterator() = default;

  /** Construct the iterator  */
  explicit unordered_map_iterator(TypedPointer<unordered_map<Key, T, Hash>> map)
  : map_(map) {}

  /** Copy constructor  */
  unordered_map_iterator(const unordered_map_iterator &other) {
    shm_strong_copy(other);
  }

  /** Assign one iterator into another */
  unordered_map_iterator<Key, T, Hash>&
  operator=(const unordered_map_iterator<Key, T, Hash> &other) {
    if (this != &other) {
      shm_strong_copy(other);
    }
    return *this;
  }

  /** Copy an iterator */
  void shm_strong_copy(const unordered_map_iterator<Key, T, Hash> &other) {
    map_ = other.map_;
    bucket_ = other.bucket_;
    collision_ = other.collision_;
  }

  /** Get the pointed object */
  hipc::ShmRef<COLLISION_T> operator*() {
    return *collision_;
  }

  /** Get the pointed object */
  const hipc::ShmRef<COLLISION_T> operator*() const {
    return *collision_;
  }

  /** Go to the next object */
  unordered_map_iterator& operator++() {
    ++collision_;
    make_correct();
    return *this;
  }

  /** Return the next iterator */
  unordered_map_iterator operator++(int) const {
    unordered_map_iterator next(*this);
    ++next;
    return next;
  }

  /**
   * Shifts bucket and collision iterator until there is a valid element.
   * Returns true if such an element is found, and false otherwise.
   * */
  bool make_correct() {
    do {
      if (bucket_.is_end()) {
        return false;
      }
      BUCKET_T& bkt = (**bucket_);
      list<COLLISION_T> &collisions = bkt;
      if (collision_ != collisions.end()) {
        return true;
      } else {
        ++bucket_;
        if (bucket_.is_end()) {
          return false;
        }
        BUCKET_T& new_bkt = (**bucket_);
        list<COLLISION_T> &new_collisions = new_bkt;
        collision_ = collisions.begin();
      }
    } while (true);
  }

  /** Check if two iterators are equal */
  friend bool operator==(const unordered_map_iterator &a,
                         const unordered_map_iterator &b) {
    if (a.is_end() && b.is_end()) {
      return true;
    }
    return (a.bucket_ == b.bucket_) && (a.collision_ == b.collision_);
  }

  /** Check if two iterators are inequal */
  friend bool operator!=(const unordered_map_iterator &a,
                         const unordered_map_iterator &b) {
    if (a.is_end() && b.is_end()) {
      return false;
    }
    return (a.bucket_ != b.bucket_) || (a.collision_ != b.collision_);
  }

  /** Determine whether this iterator is the end iterator */
  bool is_end() const {
    return bucket_.is_end();
  }

  /** Set this iterator to the end iterator */
  void set_end() {
    bucket_.set_end();
  }
};

/**
 * MACROS to simplify the unordered_map namespace
 * Used as inputs to the SHM_CONTAINER_TEMPLATE
 * */

#define CLASS_NAME unordered_map
#define TYPED_CLASS unordered_map<Key, T, Hash>
#define TYPED_HEADER ShmHeader<unordered_map<Key, T, Hash>>

/**
 * The unordered_map shared-memory header
 * */
template<typename Key, typename T, class Hash>
struct ShmHeader<TYPED_CLASS> : public ShmBaseHeader {
 public:
  using COLLISION_T = hipc::pair<Key, T>;
  using BUCKET_T = hipc::list<COLLISION_T>;

 public:
  ShmArchiveOrT<vector<BUCKET_T>> buckets_;
  RealNumber max_capacity_;
  RealNumber growth_;
  hipc::atomic<size_t> length_;

  /** Default constructor. */
  ShmHeader() = default;

  /** Constructor. Initialize header. */
  explicit ShmHeader(Allocator *alloc,
                     int num_buckets,
                     RealNumber max_capacity,
                     RealNumber growth) {
    buckets_.shm_init(alloc, num_buckets);
    max_capacity_ = max_capacity;
    growth_ = growth;
    length_ = 0;
  }

  /** Copy constructor */
  ShmHeader(const ShmHeader &other) = delete;

  /** Move constructor */
  ShmHeader(ShmHeader &&other) = delete;

  /** True move constructor. */
  explicit ShmHeader(Allocator *alloc,
                     ShmHeader &&other, Allocator *other_alloc) {
    (*GetBuckets(alloc)) = std::move(*other.GetBuckets(other_alloc));
    max_capacity_ = other.max_capacity_;
    growth_ = other.growth_;
    length_ = other.length_.load();
  }

  /** Get a reference to the buckets */
  hipc::ShmRef<vector<BUCKET_T>> GetBuckets(Allocator *alloc) {
    return hipc::ShmRef<vector<BUCKET_T>>(buckets_.internal_ref(alloc));
  }
};

/**
 * The unordered map implementation
 * */
template<typename Key, typename T, class Hash>
class unordered_map : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS), (TYPED_HEADER))
  friend unordered_map_iterator<Key, T, Hash>;

 public:
  using COLLISION_T = hipc::pair<Key, T>;
  using BUCKET_T = hipc::list<COLLISION_T>;

 public:
  ////////////////////////////
  /// SHM Overrides
  ////////////////////////////

  /** Default constructor */
  unordered_map() = default;

  /**
   * Initialize unordered map
   *
   * @param alloc the shared-memory allocator
   * @param num_buckets the number of buckets to create
   * @param max_capacity the maximum number of elements before a growth is
   * triggered
   * @param growth the multiplier to grow the bucket vector size
   * */
  void shm_init_main(TYPED_HEADER *header,
                     Allocator *alloc,
                     int num_buckets = 20,
                     RealNumber max_capacity = RealNumber(4, 5),
                     RealNumber growth = RealNumber(5, 4)) {
    shm_init_allocator(alloc);
    shm_init_header(header, alloc_, num_buckets, max_capacity, growth);
  }

  /** Store into shared memory */
  void shm_serialize_main() const {}

  /** Load from shared memory */
  void shm_deserialize_main() {}

  /** Move constructor */
  void shm_weak_move_main(TYPED_HEADER *header,
                          Allocator *alloc, unordered_map &other) {
    shm_init_allocator(alloc);
    shm_init_header(header,
                    alloc_,
                    std::move(*other.header_),
                    other.GetAllocator());
  }

  /** Copy constructor */
  void shm_strong_copy_main(TYPED_HEADER *header,
                            Allocator *alloc, const unordered_map &other) {
    auto num_buckets = other.get_num_buckets();
    auto max_capacity = other.header_->max_capacity_;
    auto growth = other.header_->growth_;
    shm_init_allocator(alloc);
    shm_init_header(header, alloc_, num_buckets, max_capacity, growth);
    for (auto entry : other) {
      emplace_templ<false, true>(
        entry->GetKey(), entry->GetVal());
    }
  }

  /** Destroy the unordered_map buckets */
  void shm_destroy_main() {
    hipc::ShmRef<vector<BUCKET_T>> buckets = GetBuckets();
    buckets->shm_destroy();
  }

  ////////////////////////////
  /// Map Operations
  ////////////////////////////

  /**
   * Construct an object directly in the map. Overrides the object if
   * key already exists.
   *
   * @param key the key to future index the map
   * @param args the arguments to construct the object
   * @return None
   * */
  template<typename ...Args>
  bool emplace(const Key &key, Args&&... args) {
    return emplace_templ<true, true>(key, std::forward<Args>(args)...);
  }

  /**
   * Construct an object directly in the map. Does not modify the key
   * if it already exists.
   *
   * @param key the key to future index the map
   * @param args the arguments to construct the object
   * @return None
   * */
  template<typename ...Args>
  bool try_emplace(const Key &key, Args&&... args) {
    return emplace_templ<true, false>(key, std::forward<Args>(args)...);
  }

  /**
   * Erase an object indexable by \a key key
   * */
  void erase(const Key &key) {
    if (header_ == nullptr) { shm_init(); }
    // Get the bucket the key belongs to
    hipc::ShmRef<vector<BUCKET_T>> buckets = GetBuckets();
    size_t bkt_id = Hash{}(key) % buckets->size();
    hipc::ShmRef<BUCKET_T> bkt = (*buckets)[bkt_id];

    // Find and remove key from collision list
    list<COLLISION_T> &collisions = *bkt;
    auto iter = find_collision(key, collisions);
    if (iter.is_end()) {
      return;
    }
    collisions.erase(iter);

    // Decrement the size of the map
    --header_->length_;
  }

  /**
   * Erase an object at the iterator
   * */
  void erase(unordered_map_iterator<Key, T, Hash> &iter) {
    if (iter == end()) return;
    // Acquire the bucket lock for a write (modifying collisions)
    hipc::ShmRef<BUCKET_T> bkt = *iter.bucket_;

    // Erase the element from the collision list
    list<COLLISION_T> &collisions = bkt;
    collisions.erase(iter.collision_);

    // Decrement the size of the map
    --header_->length_;
  }

  /**
   * Erase the entire map
   * */
  void clear() {
    hipc::ShmRef<vector<BUCKET_T>> buckets = GetBuckets();
    size_t num_buckets = buckets->size();
    buckets->clear();
    buckets->resize(num_buckets);
    header_->length_ = 0;
  }

  /**
   * Locate an entry in the unordered_map
   *
   * @return the object pointed by key
   * @exception UNORDERED_MAP_CANT_FIND the key was not in the map
   * */
  ShmRef<T> operator[](const Key &key) {
    auto iter = find(key);
    if (iter != end()) {
      return (*iter)->second_;
    }
    throw UNORDERED_MAP_CANT_FIND.format();
  }

  /**
   * Find an object in the unordered_map
   * */
  unordered_map_iterator<Key, T, Hash> find(const Key &key) {
    unordered_map_iterator<Key, T, Hash> iter(
      GetShmPointer<TypedPointer<unordered_map_iterator<Key, T, Hash>>>());

    // Determine the bucket corresponding to the key
    hipc::ShmRef<vector<BUCKET_T>> buckets = GetBuckets();
    size_t bkt_id = Hash{}(key) % buckets->size();
    iter.bucket_ = buckets->begin() + bkt_id;
    hipc::ShmRef<BUCKET_T> bkt = (*iter.bucket_);
    list<COLLISION_T> &collisions = *bkt;

    // Get the specific collision iterator
    iter.collision_ = find_collision(key, collisions);
    if (iter.collision_.is_end()) {
      iter.set_end();
    }
    return iter;
  }

  /** The number of entries in the map */
  size_t size() const {
    if (header_ == nullptr) {
      return 0;
    }
    return header_->length_.load();
  }

  /** The number of buckets in the map */
  size_t get_num_buckets() const {
    hipc::ShmRef<vector<BUCKET_T>> buckets = GetBuckets();
    return buckets->size();
  }

 private:
  /**
   * Find a key in the collision list
   * */
  list_iterator<COLLISION_T>
  find_collision(const Key &key, list<COLLISION_T> &collisions) {
    auto iter = collisions.begin();
    auto iter_end = collisions.end();
    for (; iter != iter_end; ++iter) {
      COLLISION_T entry(alloc_, **iter);
      if (entry.GetKey() == key) {
        return iter;
      }
    }
    return iter_end;
  }

  /**
   * Construct an object directly in the map
   *
   * @param key the key to future index the map
   * @param args the arguments to construct the object
   * @return None
   * */
  template<bool growth, bool modify_existing, typename ...Args>
  bool emplace_templ(const Key &key, Args&&... args) {
    COLLISION_T entry(alloc_,
                      PiecewiseConstruct(),
                      make_argpack(key),
                      make_argpack(std::forward<Args>(args)...));
    return insert_templ<growth, modify_existing>(entry);
  }

  /**
   * Insert a serialized (key, value) pair in the map
   *
   * @param growth whether or not to grow the unordered map on collision
   * @param modify_existing whether or not to override an existing entry
   * @param entry the (key,value) pair shared-memory serialized
   * @return None
   * */
  template<bool growth, bool modify_existing>
  bool insert_templ(COLLISION_T &entry) {
    if (header_ == nullptr) { shm_init(); }
    Key &key = entry.GetKey();

    // Hash the key to a bucket
    hipc::ShmRef<vector<BUCKET_T>> buckets = GetBuckets();
    size_t bkt_id = Hash{}(key) % buckets->size();
    hipc::ShmRef<BUCKET_T> bkt = (*buckets)[bkt_id];

    // Insert into the map
    list<COLLISION_T> &collisions = *bkt;
    auto has_key = find_collision(key, collisions);
    if (has_key != collisions.end()) {
      if constexpr(!modify_existing) {
        return false;
      } else {
        collisions.erase(has_key);
        collisions.emplace_back(std::move(entry));
        return true;
      }
    }
    collisions.emplace_back(std::move(entry));

    // Increment the size of the map
    ++header_->length_;
    return true;
  }

  bool insert_simple(COLLISION_T &&entry,
                     vector<BUCKET_T> &buckets) {
    if (header_ == nullptr) { shm_init(); }
    Key &key = entry.GetKey();
    size_t bkt_id = Hash{}(key) % buckets.size();
    hipc::ShmRef<BUCKET_T> bkt = buckets[bkt_id];
    list<COLLISION_T>& collisions = bkt;
    collisions.emplace_back(std::move(entry));
    return true;
  }


 public:
  ////////////////////////////
  /// Iterators
  ////////////////////////////

  /** Forward iterator begin */
  inline unordered_map_iterator<Key, T, Hash> begin() const {
    unordered_map_iterator<Key, T, Hash> iter(
      GetShmPointer<TypedPointer<unordered_map_iterator<Key, T, Hash>>>());
    hipc::ShmRef<vector<BUCKET_T>> buckets(GetBuckets());
    if (buckets->size() == 0) {
      return iter;
    }
    hipc::ShmRef<BUCKET_T> bkt = (*buckets)[0];
    list<COLLISION_T> &list = *bkt;
    iter.bucket_ = buckets->cbegin();
    iter.collision_ = list.begin();
    iter.make_correct();
    return iter;
  }

  /** Forward iterator end */
  inline unordered_map_iterator<Key, T, Hash> end() const {
    unordered_map_iterator<Key, T, Hash> iter(
      GetShmPointer<TypedPointer<unordered_map_iterator<Key, T, Hash>>>());
    hipc::ShmRef<vector<BUCKET_T>> buckets(GetBuckets());
    iter.bucket_ = buckets->cend();
    return iter;
  }

  /** Get the buckets */
  hipc::ShmRef<vector<BUCKET_T>> GetBuckets() {
    return header_->GetBuckets(alloc_);
  }

  /** Get the buckets (const) */
  hipc::ShmRef<vector<BUCKET_T>> GetBuckets() const {
    return header_->GetBuckets(alloc_);
  }
};

}  // namespace hermes_shm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_DATA_STRUCTURES_UNORDERED_MAP_H_
