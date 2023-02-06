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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_ATOMIC_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_ATOMIC_H_

#include <atomic>

namespace hermes_shm::ipc {

/** Provides the API of an atomic, without being atomic */
template<typename T>
struct nonatomic {
  T x;

  /** Constructor */
  inline nonatomic() = default;

  /** Full constructor */
  inline nonatomic(T def) : x(def) {}

  /** Atomic fetch_add wrapper*/
  inline T fetch_add(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    (void) order;
    x += count;
    return x;
  }

  /** Atomic fetch_sub wrapper*/
  inline T fetch_sub(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    (void) order;
    x -= count;
    return x;
  }

  /** Atomic load wrapper */
  inline T load(
    std::memory_order order = std::memory_order_seq_cst) const {
    (void) order;
    return x;
  }

  /** Atomic exchange wrapper */
  inline void exchange(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    (void) order;
    x = count;
  }

  /** Atomic compare exchange weak wrapper */
  inline bool compare_exchange_weak(T& expected, T desired,
                                    std::memory_order order =
                                    std::memory_order_seq_cst) {
    (void) expected; (void) order;
    x = desired;
    return true;
  }

  /** Atomic compare exchange strong wrapper */
  inline bool compare_exchange_strong(T& expected, T desired,
                                      std::memory_order order =
                                      std::memory_order_seq_cst) {
    (void) expected; (void) order;
    x = desired;
    return true;
  }

  /** Atomic pre-increment operator */
  inline nonatomic& operator++() {
    ++x;
    return *this;
  }

  /** Atomic post-increment operator */
  inline nonatomic operator++(int) {
    return atomic(x+1);
  }

  /** Atomic pre-decrement operator */
  inline nonatomic& operator--() {
    --x;
    return *this;
  }

  /** Atomic post-decrement operator */
  inline nonatomic operator--(int) {
    return atomic(x-1);
  }

  /** Atomic add operator */
  inline nonatomic operator+(T count) const {
    return nonatomic(x + count);
  }

  /** Atomic subtract operator */
  inline nonatomic operator-(T count) const {
    return nonatomic(x - count);
  }

  /** Atomic add assign operator */
  inline nonatomic& operator+=(T count) {
    x += count;
    return *this;
  }

  /** Atomic subtract assign operator */
  inline nonatomic& operator-=(T count) {
    x -= count;
    return *this;
  }

  /** Atomic assign operator */
  inline nonatomic& operator=(T count) {
    x = count;
    return *this;
  }

  /** Equality check */
  inline bool operator==(const nonatomic &other) const {
    return (other.x == x);
  }

  /** Inequality check */
  inline bool operator!=(const nonatomic &other) const {
    return (other.x != x);
  }
};

/** A wrapper around std::atomic */
template<typename T>
struct atomic {
  std::atomic<T> x;

  /** Constructor */
  inline atomic() = default;

  /** Full constructor */
  inline atomic(T def) : x(def) {}

  /** Atomic fetch_add wrapper*/
  inline T fetch_add(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    return x.fetch_add(count, order);
  }

  /** Atomic fetch_sub wrapper*/
  inline T fetch_sub(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    return x.fetch_sub(count, order);
  }

  /** Atomic load wrapper */
  inline T load(
    std::memory_order order = std::memory_order_seq_cst) const {
    return x.load(order);
  }

  /** Atomic exchange wrapper */
  inline void exchange(
    T count, std::memory_order order = std::memory_order_seq_cst) {
    x.exchange(count, order);
  }

  /** Atomic compare exchange weak wrapper */
  inline bool compare_exchange_weak(T& expected, T desired,
                                    std::memory_order order =
                                    std::memory_order_seq_cst) {
    return x.compare_exchange_weak(expected, desired, order);
  }

  /** Atomic compare exchange strong wrapper */
  inline bool compare_exchange_strong(T& expected, T desired,
                                      std::memory_order order =
                                      std::memory_order_seq_cst) {
    return x.compare_exchange_strong(expected, desired, order);
  }

  /** Atomic pre-increment operator */
  inline atomic& operator++() {
    ++x;
    return *this;
  }

  /** Atomic post-increment operator */
  inline atomic operator++(int) {
    return atomic(x+1);
  }

  /** Atomic pre-decrement operator */
  inline atomic& operator--() {
    --x;
    return *this;
  }

  /** Atomic post-decrement operator */
  inline atomic operator--(int) {
    return atomic(x-1);
  }

  /** Atomic add operator */
  inline atomic operator+(T count) const {
    return x + count;
  }

  /** Atomic subtract operator */
  inline atomic operator-(T count) const {
    return x - count;
  }

  /** Atomic add assign operator */
  inline atomic& operator+=(T count) {
    x += count;
    return *this;
  }

  /** Atomic subtract assign operator */
  inline atomic& operator-=(T count) {
    x -= count;
    return *this;
  }

  /** Atomic assign operator */
  inline atomic& operator=(T count) {
    x.exchange(count);
    return *this;
  }

  /** Equality check */
  inline bool operator==(const atomic &other) const {
    return (other.x == x);
  }

  /** Inequality check */
  inline bool operator!=(const atomic &other) const {
    return (other.x != x);
  }
};

}

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_ATOMIC_H_
