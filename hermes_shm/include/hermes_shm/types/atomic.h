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

#ifndef HERMES_INCLUDE_HERMES_TYPES_ATOMIC_H_
#define HERMES_INCLUDE_HERMES_TYPES_ATOMIC_H_

#include <atomic>
#include <hermes_shm/constants/macros.h>

namespace hshm::ipc {

/** Provides the API of an atomic, without being atomic */
template<typename T>
struct nonatomic {
  T x;

  /** Constructor */
  HSHM_ALWAYS_INLINE nonatomic() = default;

  /** Full constructor */
  HSHM_ALWAYS_INLINE explicit nonatomic(T def) : x(def) {}

  /** Atomic fetch_add wrapper*/
  HSHM_ALWAYS_INLINE T fetch_add(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    (void) order;
    x += count;
    return x;
  }

  /** Atomic fetch_sub wrapper*/
  HSHM_ALWAYS_INLINE T fetch_sub(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    (void) order;
    x -= count;
    return x;
  }

  /** Atomic load wrapper */
  HSHM_ALWAYS_INLINE T load(
    std::memory_order order = std::memory_order_seq_cst) const {
    (void) order;
    return x;
  }

  /** Atomic exchange wrapper */
  HSHM_ALWAYS_INLINE void exchange(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    (void) order;
    x = count;
  }

  /** Atomic compare exchange weak wrapper */
  HSHM_ALWAYS_INLINE bool compare_exchange_weak(T& expected, T desired,
                                    std::memory_order order =
                                    std::memory_order_seq_cst) {
    (void) expected; (void) order;
    x = desired;
    return true;
  }

  /** Atomic compare exchange strong wrapper */
  HSHM_ALWAYS_INLINE bool compare_exchange_strong(T& expected, T desired,
                                      std::memory_order order =
                                      std::memory_order_seq_cst) {
    (void) expected; (void) order;
    x = desired;
    return true;
  }

  /** Atomic pre-increment operator */
  HSHM_ALWAYS_INLINE nonatomic& operator++() {
    ++x;
    return *this;
  }

  /** Atomic post-increment operator */
  HSHM_ALWAYS_INLINE nonatomic operator++(int) {
    return atomic(x+1);
  }

  /** Atomic pre-decrement operator */
  HSHM_ALWAYS_INLINE nonatomic& operator--() {
    --x;
    return *this;
  }

  /** Atomic post-decrement operator */
  HSHM_ALWAYS_INLINE nonatomic operator--(int) {
    return atomic(x-1);
  }

  /** Atomic add operator */
  HSHM_ALWAYS_INLINE nonatomic operator+(T count) const {
    return nonatomic(x + count);
  }

  /** Atomic subtract operator */
  HSHM_ALWAYS_INLINE nonatomic operator-(T count) const {
    return nonatomic(x - count);
  }

  /** Atomic add assign operator */
  HSHM_ALWAYS_INLINE nonatomic& operator+=(T count) {
    x += count;
    return *this;
  }

  /** Atomic subtract assign operator */
  HSHM_ALWAYS_INLINE nonatomic& operator-=(T count) {
    x -= count;
    return *this;
  }

  /** Atomic assign operator */
  HSHM_ALWAYS_INLINE nonatomic& operator=(T count) {
    x = count;
    return *this;
  }

  /** Equality check */
  HSHM_ALWAYS_INLINE bool operator==(const nonatomic &other) const {
    return (other.x == x);
  }

  /** Inequality check */
  HSHM_ALWAYS_INLINE bool operator!=(const nonatomic &other) const {
    return (other.x != x);
  }
};

/** A wrapper around std::atomic */
template<typename T>
struct atomic {
  std::atomic<T> x;

  /** Constructor */
  HSHM_ALWAYS_INLINE atomic() = default;

  /** Full constructor */
  HSHM_ALWAYS_INLINE explicit atomic(T def) : x(def) {}

  /** Atomic fetch_add wrapper*/
  HSHM_ALWAYS_INLINE T fetch_add(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    return x.fetch_add(count, order);
  }

  /** Atomic fetch_sub wrapper*/
  HSHM_ALWAYS_INLINE T fetch_sub(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    return x.fetch_sub(count, order);
  }

  /** Atomic load wrapper */
  HSHM_ALWAYS_INLINE T load(
    std::memory_order order = std::memory_order_seq_cst) const {
    return x.load(order);
  }

  /** Atomic exchange wrapper */
  HSHM_ALWAYS_INLINE void exchange(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    x.exchange(count, order);
  }

  /** Atomic compare exchange weak wrapper */
  HSHM_ALWAYS_INLINE bool compare_exchange_weak(T& expected, T desired,
                                    std::memory_order order =
                                    std::memory_order_seq_cst) {
    return x.compare_exchange_weak(expected, desired, order);
  }

  /** Atomic compare exchange strong wrapper */
  HSHM_ALWAYS_INLINE bool compare_exchange_strong(T& expected, T desired,
                                      std::memory_order order =
                                      std::memory_order_seq_cst) {
    return x.compare_exchange_strong(expected, desired, order);
  }

  /** Atomic pre-increment operator */
  HSHM_ALWAYS_INLINE atomic& operator++() {
    ++x;
    return *this;
  }

  /** Atomic post-increment operator */
  HSHM_ALWAYS_INLINE atomic operator++(int) {
    return atomic(x + 1);
  }

  /** Atomic pre-decrement operator */
  HSHM_ALWAYS_INLINE atomic& operator--() {
    --x;
    return *this;
  }

  /** Atomic post-decrement operator */
  HSHM_ALWAYS_INLINE atomic operator--(int) {
    return atomic(x - 1);
  }

  /** Atomic add operator */
  HSHM_ALWAYS_INLINE atomic operator+(T count) const {
    return x + count;
  }

  /** Atomic subtract operator */
  HSHM_ALWAYS_INLINE atomic operator-(T count) const {
    return x - count;
  }

  /** Atomic add assign operator */
  HSHM_ALWAYS_INLINE atomic& operator+=(T count) {
    x += count;
    return *this;
  }

  /** Atomic subtract assign operator */
  HSHM_ALWAYS_INLINE atomic& operator-=(T count) {
    x -= count;
    return *this;
  }

  /** Atomic assign operator */
  HSHM_ALWAYS_INLINE atomic& operator=(T count) {
    x.exchange(count);
    return *this;
  }

  /** Equality check */
  HSHM_ALWAYS_INLINE bool operator==(const atomic &other) const {
    return (other.x == x);
  }

  /** Inequality check */
  HSHM_ALWAYS_INLINE bool operator!=(const atomic &other) const {
    return (other.x != x);
  }
};

}  // namespace hshm::ipc

#endif  // HERMES_INCLUDE_HERMES_TYPES_ATOMIC_H_
