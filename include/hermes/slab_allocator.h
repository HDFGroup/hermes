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

#ifndef HRUN_TASKS_HERMES_INCLUDE_HERMES_SLAB_ALLOCATOR_H_
#define HRUN_TASKS_HERMES_INCLUDE_HERMES_SLAB_ALLOCATOR_H_

#include "hrun/hrun_types.h"
#include "hermes/hermes_types.h"

namespace hermes {

struct Slab {
  size_t slab_size_;
  std::list<BufferInfo> buffers_;
};

struct SlabCount {
  size_t count_;
  size_t slab_size_;

  SlabCount() : count_(0), slab_size_(0) {}
};

class SlabAllocator {
 public:
  std::vector<Slab> slab_lists_;
  std::atomic<size_t> heap_;
  size_t dev_size_;
  TargetId target_id_;

 public:
  /** Default constructor */
  SlabAllocator() = default;

  /** Destructor */
  ~SlabAllocator() = default;

  /** Initialize slab allocator */
  void Init(TargetId target_id,
            size_t dev_size,
            std::vector<size_t> &slab_sizes) {
    heap_ = 0;
    dev_size_ = dev_size;
    target_id_ = target_id;
    slab_lists_.reserve(slab_sizes.size());
    for (auto &slab_size : slab_sizes) {
      slab_lists_.emplace_back();
      auto &slab = slab_lists_.back();
      slab.slab_size_ = slab_size;
    }
  }

  /**
   * Allocate enough slabs to ideally fit the size
   * It can allocate less than or more than the requested size
   *
   * @param size the amount of space to allocate
   * @param[OUT] buffers the buffers allocated
   * @return the amount of space actually allocated. Maximum value is size.
   * */
  void Allocate(size_t size,
                std::vector<BufferInfo> &buffers,
                size_t &total_size) {
    u32 buffer_count = 0;
    std::vector<SlabCount> coins = CoinSelect(size, buffer_count);
    buffers.reserve(buffer_count);
    total_size = 0;
    int slab_idx = 0;
    for (auto &coin : coins) {
      AllocateSlabs(coin.slab_size_,
                    slab_idx,
                    coin.count_,
                    buffers,
                    total_size);
      ++slab_idx;
    }
  }

 private:
  /** Determine how many of each slab size to allocate */
  std::vector<SlabCount> CoinSelect(size_t size, u32 &buffer_count) {
    std::vector<SlabCount> coins(slab_lists_.size());
    size_t rem_size = size;

    while (rem_size) {
      // Find the slab size nearest to the rem_size
      size_t i = 0, slab_size = 0;
      for (auto &slab : slab_lists_) {
        if (slab.slab_size_ >= rem_size) {
          break;
        }
        ++i;
      }
      if (i == slab_lists_.size()) { i -= 1; }
      slab_size = slab_lists_[i].slab_size_;

      // Divide rem_size into slabs
      if (rem_size > slab_size) {
        coins[i].count_ += rem_size / slab_size;
        coins[i].slab_size_ = slab_size;
        rem_size %= slab_size;
      } else {
        coins[i].count_ += 1;
        coins[i].slab_size_ = slab_size;
        rem_size = 0;
      }
      buffer_count += coins[i].count_;
    }

    return coins;
  }

  /** Allocate slabs of a certain size */
  void AllocateSlabs(size_t slab_size, int slab_idx, size_t count,
                     std::vector<BufferInfo> &buffers,
                     size_t &total_size) {
    auto &slab = slab_lists_[slab_idx];
    for (size_t i = 0; i < count; ++i) {
      if (slab.buffers_.size() > 0) {
        buffers.push_back(slab.buffers_.front());
        slab.buffers_.pop_front();
      } else {
        // Verify the heap has space
        if (heap_.load() + slab_size > dev_size_) {
          return;
        }
        // Allocate a new slab
        buffers.emplace_back();
        BufferInfo &buf = buffers.back();
        buf.tid_ = target_id_;
        buf.t_off_ = heap_.fetch_add(slab_size);
        buf.t_size_ = slab_size;
        buf.t_slab_ = slab_idx;
      }
      total_size += slab_size;
    }
  }

 public:
  /** Free a set of buffers */
  void Free(const std::vector<BufferInfo> &buffers) {
    for (const auto &buffer : buffers) {
      auto &slab = slab_lists_[buffer.t_slab_];
      slab.buffers_.push_back(buffer);
    }
  }
};

}  // namespace hermes

#endif  // HRUN_TASKS_HERMES_INCLUDE_HERMES_SLAB_ALLOCATOR_H_
