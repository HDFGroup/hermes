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

#include "hermes_shm/thread/thread_model_manager.h"
#include "hermes_shm/data_structures/ipc/vector.h"
#include "hermes_shm/data_structures/ipc/slist.h"
#include "pair.h"
#include "hermes_shm/types/atomic.h"
#include "hermes_shm/data_structures/ipc/internal/shm_internal.h"

namespace hshm::ipc {

/** forward pointer for unordered_map */
template<typename Key, typename T, class Hash = std::hash<Key>>
class unordered_map;

/**
 * The unordered map iterator (bucket_iter, slist_iter)
 * */
template<typename Key, typename T, class Hash>
struct unordered_map_iterator {
 public:
  using COLLISION_T = hipc::pair<Key, T>;
  using BUCKET_T = hipc::slist<COLLISION_T>;

 public:
  unordered_map<Key, T, Hash> *map_;
  typename vector<BUCKET_T>::iterator_t bucket_;
  typename slist<COLLISION_T>::iterator_t collision_;

  /** Default constructor */
  unordered_map_iterator() = default;

  /** Construct the iterator  */
  HSHM_ALWAYS_INLINE explicit unordered_map_iterator(
    unordered_map<Key, T, Hash> &map)
  : map_(&map) {}

  /** Copy constructor  */
  HSHM_ALWAYS_INLINE unordered_map_iterator(
    const unordered_map_iterator &other) {
    shm_strong_copy(other);
  }

  /** Assign one iterator into another */
  HSHM_ALWAYS_INLINE unordered_map_iterator&
  operator=(const unordered_map_iterator &other) {
    if (this != &other) {
      shm_strong_copy(other);
    }
    return *this;
  }

  /** Copy an iterator */
  void shm_strong_copy(const unordered_map_iterator &other) {
    map_ = other.map_;
    bucket_ = other.bucket_;
    collision_ = other.collision_;
  }

  /** Get the pointed object */
  HSHM_ALWAYS_INLINE COLLISION_T& operator*() {
    return *collision_;
  }

  /** Get the pointed object */
  HSHM_ALWAYS_INLINE const COLLISION_T& operator*() const {
    return *collision_;
  }

  /** Go to the next object */
  HSHM_ALWAYS_INLINE unordered_map_iterator& operator++() {
    ++collision_;
    make_correct();
    return *this;
  }

  /** Return the next iterator */
  HSHM_ALWAYS_INLINE unordered_map_iterator operator++(int) const {
    unordered_map_iterator next(*this);
    ++next;
    return next;
  }

  /**
   * Shifts bucket and collision iterator until there is a valid element.
   * Returns true if such an element is found, and false otherwise.
   * */
  HSHM_ALWAYS_INLINE bool make_correct() {
    do {
      if (bucket_.is_end()) {
        return false;
      }
      if (!collision_.is_end()) {
        return true;
      } else {
        ++bucket_;
        if (bucket_.is_end()) {
          return false;
        }
        BUCKET_T &bkt = *bucket_;
        collision_ = bkt.begin();
      }
    } while (true);
  }

  /** Check if two iterators are equal */
  HSHM_ALWAYS_INLINE friend bool operator==(
    const unordered_map_iterator &a, const unordered_map_iterator &b) {
    if (a.is_end() && b.is_end()) {
      return true;
    }
    return (a.bucket_ == b.bucket_) && (a.collision_ == b.collision_);
  }

  /** Check if two iterators are inequal */
  HSHM_ALWAYS_INLINE friend bool operator!=(
    const unordered_map_iterator &a, const unordered_map_iterator &b) {
    if (a.is_end() && b.is_end()) {
      return false;
    }
    return (a.bucket_ != b.bucket_) || (a.collision_ != b.collision_);
  }

  /** Determine whether this iterator is the end iterator */
  HSHM_ALWAYS_INLINE bool is_end() const {
    return bucket_.is_end();
  }

  /** Set this iterator to the end iterator */
  HSHM_ALWAYS_INLINE void set_end() {
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
 * The unordered map implementation
 * */
template<typename Key, typename T, class Hash>
class unordered_map : public ShmContainer {
 public:
  SHM_CONTAINER_TEMPLATE((CLASS_NAME), (TYPED_CLASS))

  /**====================================
   * Typedefs
   * ===================================*/
  typedef unordered_map_iterator<Key, T, Hash> iterator_t;
  friend iterator_t;
  using COLLISION_T = hipc::pair<Key, T>;
  using BUCKET_T = hipc::slist<COLLISION_T>;

  /**====================================
   * Variables
   * ===================================*/
  ShmArchive<vector<BUCKET_T>> buckets_;
  RealNumber max_capacity_;
  RealNumber growth_;
  hipc::atomic<size_t> length_;

 public:
  /**====================================
   * Default Constructor
   * ===================================*/

  /**
   * SHM constructor. Initialize the map.
   *
   * @param alloc the shared-memory allocator
   * @param num_buckets the number of buckets to create
   * @param max_capacity the maximum number of elements before a growth is
   * triggered
   * @param growth the multiplier to grow the bucket vector size
   * */
  explicit unordered_map(Allocator *alloc,
                         int num_buckets = 20,
                         RealNumber max_capacity = RealNumber(4, 5),
                         RealNumber growth = RealNumber(5, 4)) {
    shm_init_container(alloc);
    HSHM_MAKE_AR(buckets_, GetAllocator(), num_buckets)
    max_capacity_ = max_capacity;
    growth_ = growth;
    length_ = 0;
  }

  /**====================================
   * Copy Constructors
   * ===================================*/

  /** SHM copy constructor */
  explicit unordered_map(Allocator *alloc,
                         const unordered_map &other) {
    shm_init_container(alloc);
    shm_strong_copy_construct(other);
  }

  /** SHM copy constructor main */
  void shm_strong_copy_construct(const unordered_map &other) {
    SetNull();
    HSHM_MAKE_AR(buckets_, GetAllocator(), other.GetBuckets())
    shm_strong_copy_construct_and_op(other);
  }

  /** SHM copy assignment operator */
  unordered_map& operator=(const unordered_map &other) {
    if (this != &other) {
      shm_destroy();
      shm_strong_copy_op(other);
    }
    return *this;
  }

  /** SHM copy assignment main */
  void shm_strong_copy_op(const unordered_map &other) {
    int num_buckets = other.get_num_buckets();
    GetBuckets().resize(num_buckets);
    shm_strong_copy_construct_and_op(other);
  }

  /** Internal copy operation */
  void shm_strong_copy_construct_and_op(const unordered_map &other) {
    max_capacity_ = other.max_capacity_;
    growth_ = other.growth_;
    for (hipc::pair<Key, T> &entry : other) {
      emplace_templ<false, true>(
        entry.GetKey(), entry.GetVal());
    }
  }

  /**====================================
   * Move Constructors
   * ===================================*/

  /** SHM move constructor. */
  HSHM_ALWAYS_INLINE unordered_map(Allocator *alloc,
                                   unordered_map &&other) noexcept {
    shm_init_container(alloc);
    if (GetAllocator() == other.GetAllocator()) {
      strong_copy(other);
      HSHM_MAKE_AR(buckets_, GetAllocator(), std::move(other.GetBuckets()))
      other.SetNull();
    } else {
      shm_strong_copy_construct(other);
      other.shm_destroy();
    }
  }

  /** Copy */
  HSHM_ALWAYS_INLINE void strong_copy(const unordered_map &other) {
    max_capacity_ = other.max_capacity_;
    growth_ = other.growth_;
    length_ = other.length_.load();
  }

  /** SHM move assignment operator. */
  unordered_map& operator=(unordered_map &&other) noexcept {
    if (this != &other) {
      shm_destroy();
      if (GetAllocator() == other.GetAllocator()) {
        GetBuckets() = std::move(other.GetBuckets());
        strong_copy(other);
        other.SetNull();
      } else {
        shm_strong_copy_op(other);
        other.shm_destroy();
      }
    }
    return *this;
  }

  /**====================================
   * Destructor
   * ===================================*/

  /** Check if the pair is empty */
  HSHM_ALWAYS_INLINE bool IsNull() {
    return length_.load() == 0;
  }

  /** Sets this pair as empty */
  HSHM_ALWAYS_INLINE void SetNull() {
    length_ = 0;
  }

  /** Destroy the unordered_map buckets */
  HSHM_ALWAYS_INLINE void shm_destroy_main() {
    vector<BUCKET_T>& buckets = GetBuckets();
    buckets.shm_destroy();
  }

  /**====================================
   * Emplace Methods
   * ===================================*/

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

 private:
  /**
   * Insert a serialized (key, value) pair in the map
   *
   * @param growth whether or not to grow the unordered map on collision
   * @param modify_existing whether or not to override an existing entry
   * @param entry the (key,value) pair shared-memory serialized
   * @return None
   * */
  template<bool growth, bool modify_existing, typename ...Args>
  HSHM_ALWAYS_INLINE bool emplace_templ(const Key &key, Args&& ...args) {
    // Hash the key to a bucket
    vector<BUCKET_T>& buckets = GetBuckets();
    size_t bkt_id = Hash{}(key) % buckets.size();
    BUCKET_T& bkt = (buckets)[bkt_id];

    // Insert into the map
    auto has_key_iter = find_collision(key, bkt);
    if (!has_key_iter.is_end()) {
      if constexpr(!modify_existing) {
        return false;
      } else {
        bkt.erase(has_key_iter);
        --length_;
      }
    }
    bkt.emplace_back(PiecewiseConstruct(),
                     make_argpack(key),
                     make_argpack(std::forward<Args>(args)...));

    // Increment the size of the map
    ++length_;
    return true;
  }

 public:
  /**====================================
   * Erase Methods
   * ===================================*/

  /**
   * Erase an object indexable by \a key key
   * */
  void erase(const Key &key) {
    // Get the bucket the key belongs to
    vector<BUCKET_T>& buckets = GetBuckets();
    size_t bkt_id = Hash{}(key) % buckets.size();
    BUCKET_T& bkt = (buckets)[bkt_id];

    // Find and remove key from collision slist
    auto iter = find_collision(key, bkt);
    if (iter.is_end()) {
      return;
    }
    bkt.erase(iter);

    // Decrement the size of the map
    --length_;
  }

  /**
   * Erase an object at the iterator
   * */
  void erase(iterator_t &iter) {
    if (iter == end()) return;
    // Acquire the bucket lock for a write (modifying collisions)
    BUCKET_T& bkt = *iter.bucket_;

    // Erase the element from the collision slist
    bkt.erase(iter.collision_);

    // Decrement the size of the map
    --length_;
  }

  /**
   * Erase the entire map
   * */
  void clear() {
    vector<BUCKET_T>& buckets = GetBuckets();
    size_t num_buckets = buckets.size();
    buckets.clear();
    buckets.resize(num_buckets);
    length_ = 0;
  }

  /**====================================
   * Index Methods
   * ===================================*/

  /**
   * Locate an entry in the unordered_map
   *
   * @return the object pointed by key
   * @exception UNORDERED_MAP_CANT_FIND the key was not in the map
   * */
  HSHM_ALWAYS_INLINE T& operator[](const Key &key) {
    auto iter = find(key);
    if (!iter.is_end()) {
      return (*iter).second_.get_ref();
    }
    throw UNORDERED_MAP_CANT_FIND.format();
  }

  /** Find an object in the unordered_map */
  iterator_t find(const Key &key) {
    iterator_t iter(*this);

    // Determine the bucket corresponding to the key
    vector<BUCKET_T>& buckets = GetBuckets();
    size_t bkt_id = Hash{}(key) % buckets.size();
    iter.bucket_ = buckets.begin() + bkt_id;
    BUCKET_T& bkt = (*iter.bucket_);

    // Get the specific collision iterator
    iter.collision_ = find_collision(key, bkt);
    if (iter.collision_.is_end()) {
      iter.set_end();
    }
    return iter;
  }

  /** Find a key in the collision slist */
  typename BUCKET_T::iterator_t
  HSHM_ALWAYS_INLINE find_collision(const Key &key, BUCKET_T &bkt) {
    auto iter = bkt.begin();
    auto iter_end = bkt.end();
    for (; iter != iter_end; ++iter) {
      COLLISION_T &collision = *iter;
      if (collision.GetKey() == key) {
        return iter;
      }
    }
    return iter_end;
  }

  /**====================================
   * Query Methods
   * ===================================*/

  /** The number of entries in the map */
  HSHM_ALWAYS_INLINE size_t size() const {
    return length_.load();
  }

  /** The number of buckets in the map */
  HSHM_ALWAYS_INLINE size_t get_num_buckets() const {
    vector<BUCKET_T>& buckets = GetBuckets();
    return buckets.size();
  }

 public:
  /**====================================
   * Iterators
   * ===================================*/

  /** Forward iterator begin */
  HSHM_ALWAYS_INLINE iterator_t begin() const {
    iterator_t iter(const_cast<unordered_map&>(*this));
    vector<BUCKET_T>& buckets(GetBuckets());
    if (buckets.size() == 0) {
      return iter;
    }
    BUCKET_T& bkt = buckets[0];
    iter.bucket_ = buckets.cbegin();
    iter.collision_ = bkt.begin();
    iter.make_correct();
    return iter;
  }

  /** Forward iterator end */
  HSHM_ALWAYS_INLINE iterator_t end() const {
    iterator_t iter(const_cast<unordered_map&>(*this));
    vector<BUCKET_T>& buckets(GetBuckets());
    iter.bucket_ = buckets.cend();
    return iter;
  }

  /** Get the buckets */
  HSHM_ALWAYS_INLINE vector<BUCKET_T>& GetBuckets() {
    return *buckets_;
  }

  /** Get the buckets (const) */
  HSHM_ALWAYS_INLINE vector<BUCKET_T>& GetBuckets() const {
    return const_cast<vector<BUCKET_T>&>(*buckets_);
  }
};

}  // namespace hshm::ipc

#undef CLASS_NAME
#undef TYPED_CLASS
#undef TYPED_HEADER

#endif  // HERMES_DATA_STRUCTURES_UNORDERED_MAP_H_
