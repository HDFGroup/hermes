//
// Created by lukemartinlogan on 2/20/24.
//

#ifndef HERMES_HRUN_INCLUDE_HRUN_HRUN_MAP_H_
#define HERMES_HRUN_INCLUDE_HRUN_HRUN_MAP_H_

#include "hrun_types.h"

namespace hrun {

template<typename Key, typename T>
class LockFreeMap;

/**
 * Iterator for lock-free map
 * */
template<typename Key, typename T>
struct LockFreeMapIterator {
 public:
  using Pair = hipc::pair<Key, T>;
  size_t x_, y_;
  Pair *val_;

  bool operator==(const LockFreeMapIterator &other) const {
    return x_ == other.x_ && y_ == other.y_;
  }

  bool operator!=(const LockFreeMapIterator &other) const {
    return x_ != other.x_ || y_ != other.y_;
  }
};

/**
 * A lock-free unordered map implementation
 * */
template<typename Key, typename T>
class LockFreeMap {
 public:
  using Pair = hipc::pair<Key, T>;
  using Bucket = hipc::mpsc_queue<Pair>;
  hipc::mptr<hipc::vector<Bucket>> map_;
  int num_lanes_;
  size_t bkts_per_lane_;

  void Init(hipc::Allocator *alloc, int num_lanes, size_t bkts_per_lane,
            size_t collisions) {
    map_ = hipc::make_mptr<hipc::vector<Bucket>>(
        alloc, num_lanes * bkts_per_lane, collisions);
  }

  void Connect(const hipc::Pointer &p) {
    map_ = hipc::mptr<hipc::vector<Bucket>>(
        p);
  }

  size_t FirstBucket(int lane_id) const {
    return lane_id * bkts_per_lane_;
  }

  size_t LastBucket(int lane_id) const {
    return FirstBucket(lane_id) + bkts_per_lane_;
  }

  Bucket& GetBucket(size_t bkt_id) {
    return (*map_)[bkt_id];
  }

  hipc::Pointer GetShmPointer() {
    return map_->template GetShmPointer<hipc::Pointer>();
  }

  void emplace(const Key &key, const T &val) {
    size_t hash = std::hash<Key>{}(key);
    size_t x = hash % map_->size();
    Bucket &bkt = (*map_)[x];
    bkt.emplace(key, val);
  }

  LockFreeMapIterator<Key, T> find(const Key &key) {
    LockFreeMapIterator<Key, T> it;
    size_t hash = std::hash<Key>{}(key);
    it.x_ = hash % map_->size();
    Bucket &bkt = (*map_)[it.x_];
    size_t head = bkt.head_;
    size_t tail = bkt.tail_;
    size_t bkt_size = 0;
    if (head <= tail) {
      bkt_size = tail - head;
    }
    for (size_t i = 0; i < bkt_size; ++i) {
      Pair *val;
      if (bkt.peek(val, i).IsNull()) {
        continue;
      }
      it.y_ = i;
      it.val_ = val;
      return it;
    }
    it.x_ = map_->size();
    it.y_ = -1;
    return it;
  }

  T& operator[](const Key &key) {
    LockFreeMapIterator<Key, T> it = find(key);
    if (it.x_ == map_->size()) {
      throw std::runtime_error("Key not found");
    }
    return *it.val_->second_;
  }

  LockFreeMapIterator<Key, T> end() const {
    LockFreeMapIterator<Key, T> it;
    it.x_ = map_->size();
    it.y_ = -1;
    return it;
  }
};

}  // namespace hshm

#endif //HERMES_HRUN_INCLUDE_HRUN_HRUN_MAP_H_
