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


#ifndef HERMES_MEMORY_MEMORY_H_
#define HERMES_MEMORY_MEMORY_H_

#include <hermes_shm/types/basic.h>
#include <hermes_shm/constants/data_structure_singleton_macros.h>
#include <hermes_shm/introspect/system_info.h>
#include <hermes_shm/types/bitfield.h>
#include <hermes_shm/types/atomic.h>

namespace hermes_shm::ipc {

/**
 * The identifier for an allocator
 * */
union allocator_id_t {
  struct {
    uint32_t major_;  // Typically some sort of process id
    uint32_t minor_;  // Typically a process-local id
  } bits_;
  uint64_t int_;

  /**
   * Null allocator ID is -1 (for now)
   * */
  allocator_id_t() : int_(0) {}

  /**
   * Constructor which sets major & minor
   * */
  explicit allocator_id_t(uint32_t major, uint32_t minor) {
    bits_.major_ = major;
    bits_.minor_ = minor;
  }

  /**
   * Set this allocator to null
   * */
  void SetNull() {
    int_ = 0;
  }

  /**
   * Check if this is the null allocator
   * */
  bool IsNull() const { return int_ == 0; }

  /** Equality check */
  bool operator==(const allocator_id_t &other) const {
    return other.int_ == int_;
  }

  /** Inequality check */
  bool operator!=(const allocator_id_t &other) const {
    return other.int_ != int_;
  }

  /** Get the null allocator */
  static allocator_id_t GetNull() {
    static allocator_id_t alloc(0, 0);
    return alloc;
  }
};

typedef uint32_t slot_id_t;  // Uniquely ids a MemoryBackend slot

/**
 * Stores an offset into a memory region. Assumes the developer knows
 * which allocator the pointer comes from.
 * */
template<bool ATOMIC=false>
struct OffsetPointerBase {
  typedef typename std::conditional<ATOMIC,
    atomic<size_t>, nonatomic<size_t>>::type atomic_t;
  atomic_t off_; /**< Offset within the allocator's slot */

  /** Default constructor */
  OffsetPointerBase() = default;

  /** Full constructor */
  explicit OffsetPointerBase(size_t off) : off_(off) {}

  /** Full constructor */
  explicit OffsetPointerBase(atomic_t off) : off_(off.load()) {}

  /** Pointer constructor */
  explicit OffsetPointerBase(allocator_id_t alloc_id, size_t off) : off_(off) {
    (void) alloc_id;
  }

  /** Copy constructor */
  OffsetPointerBase(const OffsetPointerBase &other)
  : off_(other.off_.load()) {}

  /** Other copy constructor */
  OffsetPointerBase(const OffsetPointerBase<!ATOMIC> &other)
  : off_(other.off_.load()) {}

  /** Move constructor */
  OffsetPointerBase(OffsetPointerBase &&other) noexcept
    : off_(other.off_.load()) {
    other.SetNull();
  }

  /** Get the offset pointer */
  OffsetPointerBase<false> ToOffsetPointer() {
    return OffsetPointerBase<false>(off_.load());
  }

  /** Set to null */
  void SetNull() {
    off_ = -1;
  }

  /** Check if null */
  bool IsNull() const {
    return off_ == -1;
  }

  /** Get the null pointer */
  static OffsetPointerBase GetNull() {
    const static OffsetPointerBase p(-1);
    return p;
  }

  /** Atomic load wrapper */
  inline size_t load(
    std::memory_order order = std::memory_order_seq_cst) const {
    return off_.load(order);
  }

  /** Atomic exchange wrapper */
  inline void exchange(
    size_t count, std::memory_order order = std::memory_order_seq_cst) {
    off_.exchange(count, order);
  }

  /** Atomic compare exchange weak wrapper */
  inline bool compare_exchange_weak(size_t& expected, size_t desired,
                                    std::memory_order order =
                                    std::memory_order_seq_cst) {
    return off_.compare_exchange_weak(expected, desired, order);
  }

  /** Atomic compare exchange strong wrapper */
  inline bool compare_exchange_strong(size_t& expected, size_t desired,
                                      std::memory_order order =
                                      std::memory_order_seq_cst) {
    return off_.compare_exchange_weak(expected, desired, order);
  }

  /** Atomic add operator */
  inline OffsetPointerBase operator+(size_t count) const {
    return OffsetPointerBase(off_ + count);
  }

  /** Atomic subtract operator */
  inline OffsetPointerBase operator-(size_t count) const {
    return OffsetPointerBase(off_ - count);
  }

  /** Atomic add assign operator */
  inline OffsetPointerBase& operator+=(size_t count) {
    off_ += count;
    return *this;
  }

  /** Atomic subtract assign operator */
  inline OffsetPointerBase& operator-=(size_t count) {
    off_ -= count;
    return *this;
  }

  /** Atomic assign operator */
  inline OffsetPointerBase& operator=(size_t count) {
    off_ = count;
    return *this;
  }

  /** Atomic copy assign operator */
  inline OffsetPointerBase& operator=(const OffsetPointerBase &count) {
    off_ = count.load();
    return *this;
  }

  /** Equality check */
  bool operator==(const OffsetPointerBase &other) const {
    return off_ == other.off_;
  }

  /** Inequality check */
  bool operator!=(const OffsetPointerBase &other) const {
    return off_ != other.off_;
  }
};

/** Non-atomic offset */
typedef OffsetPointerBase<false> OffsetPointer;

/** Atomic offset */
typedef OffsetPointerBase<true> AtomicOffsetPointer;

/** Typed offset pointer */
template<typename T>
using TypedOffsetPointer = OffsetPointer;

/** Typed atomic pointer */
template<typename T>
using TypedAtomicOffsetPointer = AtomicOffsetPointer;

/**
 * A process-independent pointer, which stores both the allocator's
 * information and the offset within the allocator's region
 * */
template<bool ATOMIC=false>
struct PointerBase {
  allocator_id_t allocator_id_;     /// Allocator the pointer comes from
  OffsetPointerBase<ATOMIC> off_;   /// Offset within the allocator's slot

  /** Default constructor */
  PointerBase() = default;

  /** Full constructor */
  explicit PointerBase(allocator_id_t id, size_t off) :
    allocator_id_(id), off_(off) {}

  /** Full constructor using offset pointer */
  explicit PointerBase(allocator_id_t id, OffsetPointer off) :
    allocator_id_(id), off_(off) {}

  /** Copy constructor */
  PointerBase(const PointerBase &other)
  : allocator_id_(other.allocator_id_), off_(other.off_) {}

  /** Other copy constructor */
  PointerBase(const PointerBase<!ATOMIC> &other)
  : allocator_id_(other.allocator_id_), off_(other.off_.load()) {}

  /** Move constructor */
  PointerBase(PointerBase &&other) noexcept
  : allocator_id_(other.allocator_id_), off_(other.off_) {
    other.SetNull();
  }

  /** Get the offset pointer */
  OffsetPointerBase<false> ToOffsetPointer() const {
    return OffsetPointerBase<false>(off_.load());
  }

  /** Set to null */
  void SetNull() {
    allocator_id_.SetNull();
  }

  /** Check if null */
  bool IsNull() const {
    return allocator_id_.IsNull();
  }

  /** Get the null pointer */
  static PointerBase GetNull() {
    const static PointerBase p(allocator_id_t::GetNull(),
                               OffsetPointer::GetNull());
    return p;
  }

  /** Copy assignment operator */
  PointerBase& operator=(const PointerBase &other) {
    if (this != &other) {
      allocator_id_ = other.allocator_id_;
      off_ = other.off_;
    }
    return *this;
  }

  /** Move assignment operator */
  PointerBase& operator=(PointerBase &&other) {
    if (this != &other) {
      allocator_id_ = other.allocator_id_;
      off_.exchange(other.off_.load());
      other.SetNull();
    }
    return *this;
  }

  /** Addition operator */
  PointerBase operator+(size_t size) const {
    PointerBase p;
    p.allocator_id_ = allocator_id_;
    p.off_ = off_ + size;
    return p;
  }

  /** Subtraction operator */
  PointerBase operator-(size_t size) const {
    PointerBase p;
    p.allocator_id_ = allocator_id_;
    p.off_ = off_ - size;
    return p;
  }

  /** Addition assignment operator */
  PointerBase& operator+=(size_t size) {
    off_ += size;
    return *this;
  }

  /** Subtraction assignment operator */
  PointerBase& operator-=(size_t size) {
    off_ -= size;
    return *this;
  }

  /** Equality check */
  bool operator==(const PointerBase &other) const {
    return (other.allocator_id_ == allocator_id_ && other.off_ == off_);
  }

  /** Inequality check */
  bool operator!=(const PointerBase &other) const {
    return (other.allocator_id_ != allocator_id_ || other.off_ != off_);
  }
};

/** Non-atomic pointer */
typedef PointerBase<false> Pointer;

/** Atomic pointer */
typedef PointerBase<true> AtomicPointer;

/** Typed pointer */
template<typename T>
using TypedPointer = Pointer;

/** Typed atomic pointer */
template<typename T>
using TypedAtomicPointer = AtomicPointer;

/** Round up to the nearest multiple of the alignment */
static size_t NextAlignmentMultiple(size_t alignment, size_t size) {
  auto page_size = HERMES_SYSTEM_INFO->page_size_;
  size_t new_size = size;
  size_t page_off = size % alignment;
  if (page_off) {
    new_size = size + page_size - page_off;
  }
  return new_size;
}

/** Round up to the nearest multiple of page size */
static size_t NextPageSizeMultiple(size_t size) {
  auto page_size = HERMES_SYSTEM_INFO->page_size_;
  size_t new_size = NextAlignmentMultiple(page_size, size);
  return new_size;
}

}  // namespace hermes_shm::ipc

namespace std {

/** Allocator ID hash */
template <>
struct hash<hermes_shm::ipc::allocator_id_t> {
  std::size_t operator()(const hermes_shm::ipc::allocator_id_t &key) const {
    return std::hash<uint64_t>{}(key.int_);
  }
};

}  // namespace std


#endif  // HERMES_MEMORY_MEMORY_H_
