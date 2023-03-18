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

#ifndef HERMES_INCLUDE_HERMES_DATA_STRUCTURES_TupleBase_H_
#define HERMES_INCLUDE_HERMES_DATA_STRUCTURES_TupleBase_H_

#include <utility>
#include "hermes_shm/types/basic.h"
#include "hermes_shm/types/argpack.h"

namespace hermes_shm {

/** The null container wrapper */
template<typename T>
using NullWrap = T;

/** Recurrence used to create argument pack */
template<
  template<typename> typename Wrap,
  size_t idx,
  typename T = EndTemplateRecurrence,
  typename ...Args>
struct TupleBaseRecur {
  Wrap<T> arg_; /**< The element stored */
  TupleBaseRecur<Wrap, idx + 1, Args...>
    recur_; /**< Remaining args */

  /** Default constructor */
  TupleBaseRecur() = default;

  /** Constructor. Const reference. */
  explicit TupleBaseRecur(const T &arg, Args&& ...args)
    : arg_(std::forward<T>(arg)), recur_(std::forward<Args>(args)...) {}

  /** Constructor. Lvalue reference. */
  explicit TupleBaseRecur(T& arg, Args&& ...args)
  : arg_(std::forward<T>(arg)), recur_(std::forward<Args>(args)...) {}

  /** Constructor. Rvalue reference. */
  explicit TupleBaseRecur(T&& arg, Args&& ...args)
  : arg_(std::forward<T>(arg)), recur_(std::forward<Args>(args)...) {}

  /** Move constructor */
  TupleBaseRecur(TupleBaseRecur &&other) noexcept
  : arg_(std::move(other.arg_)), recur_(std::move(other.recur_)) {}

  /** Move assignment operator */
  TupleBaseRecur& operator=(TupleBaseRecur &&other) {
    if (this != &other) {
      arg_ = std::move(other.arg_);
      recur_ = std::move(other.recur_);
    }
    return *this;
  }

  /** Copy constructor */
  TupleBaseRecur(const TupleBaseRecur &other)
  : arg_(other.arg_), recur_(other.recur_) {}

  /** Copy assignment operator */
  TupleBaseRecur& operator=(const TupleBaseRecur &other) {
    if (this != &other) {
      arg_ = other.arg_;
      recur_ = other.recur_;
    }
    return *this;
  }

  /** Solidification constructor */
  template<typename ...CArgs>
  explicit TupleBaseRecur(ArgPack<CArgs...> &&other)
    : arg_(other.template Forward<idx>()),
      recur_(std::forward<ArgPack<CArgs...>>(other)) {}

  /** Get reference to internal variable (only if tuple) */
  template<size_t i>
  constexpr auto& Get() {
    if constexpr(i == idx) {
      return arg_;
    } else {
      return recur_.template
        Get<i>();
    }
  }

  /** Get reference to internal variable (only if tuple, const) */
  template<size_t i>
  constexpr auto& Get() const {
    if constexpr(i == idx) {
      return arg_;
    } else {
      return recur_.template
        Get<i>();
    }
  }
};

/** Terminator of the TupleBase recurrence */
template<
  template<typename> typename Wrap,
  size_t idx>
struct TupleBaseRecur<Wrap, idx, EndTemplateRecurrence> {
  /** Default constructor */
  TupleBaseRecur() = default;

  /** Solidification constructor */
  template<typename ...CArgs>
  explicit TupleBaseRecur(ArgPack<CArgs...> &&other) {}

  /** Getter */
  template<size_t i>
  void Get() {
    throw std::logic_error("(Get) TupleBase index outside of range");
  }

  /** Getter */
  template<size_t i>
  void Get() const {
    throw std::logic_error("(Get) TupleBase index outside of range");
  }
};

/** Used to semantically pack arguments */
template<
  bool is_argpack,
  template<typename> typename Wrap,
  typename ...Args>
struct TupleBase {
  /** Variable argument pack */
  TupleBaseRecur<Wrap, 0, Args...> recur_;

  /** Default constructor */
  TupleBase() = default;

  /** General Constructor. */
  template<typename ...CArgs>
  explicit TupleBase(Args&& ...args)
  : recur_(std::forward<Args>(args)...) {}

  /** Move constructor */
  TupleBase(TupleBase &&other) noexcept
  : recur_(std::move(other.recur_)) {}

  /** Move assignment operator */
  TupleBase& operator=(TupleBase &&other) noexcept {
    if (this != &other) {
      recur_ = std::move(other.recur_);
    }
    return *this;
  }

  /** Copy constructor */
  TupleBase(const TupleBase &other)
  : recur_(other.recur_) {}

  /** Copy assignment operator */
  TupleBase& operator=(const TupleBase &other) {
    if (this != &other) {
      recur_ = other.recur_;
    }
    return *this;
  }

  /** Solidification constructor */
  template<typename ...CArgs>
  explicit TupleBase(ArgPack<CArgs...> &&other)
  : recur_(std::forward<ArgPack<CArgs...>>(other)) {}

  /** Getter */
  template<size_t idx>
  constexpr auto& Get() {
    return recur_.template Get<idx>();
  }

  /** Getter (const) */
  template<size_t idx>
  constexpr auto& Get() const {
    return recur_.template Get<idx>();
  }

  /** Size */
  constexpr static size_t Size() {
    return sizeof...(Args);
  }
};

/** Tuple definition */
template<typename ...Containers>
using tuple = TupleBase<false, NullWrap, Containers...>;

/** Tuple Wrapper Definition */
template<template<typename> typename Wrap, typename ...Containers>
using tuple_wrap = TupleBase<false, Wrap, Containers...>;

/** Used to emulate constexpr to lambda */
template<typename T, T Val>
struct MakeConstexpr {
  constexpr static T val_ = Val;
  constexpr static T Get() {
    return val_;
  }
};

/** Apply a function over an entire TupleBase / tuple */
template<bool reverse>
class IterateTuple {
 public:
  /** Apply a function to every element of a tuple */
  template<typename TupleT, typename F>
  constexpr static void Apply(TupleT &pack, F &&f) {
    _Apply<0, TupleT, F>(pack, std::forward<F>(f));
  }

 private:
  /** Apply the function recursively */
  template<size_t i, typename TupleT, typename F>
  constexpr static void _Apply(TupleT &pack, F &&f) {
    if constexpr(i < TupleT::Size()) {
      if constexpr(reverse) {
        _Apply<i + 1, TupleT, F>(pack, std::forward<F>(f));
        f(MakeConstexpr<size_t, i>(), pack.template Get<i>());
      } else {
        f(MakeConstexpr<size_t, i>(), pack.template Get<i>());
        _Apply<i + 1, TupleT, F>(pack, std::forward<F>(f));
      }
    }
  }
};

/** Forward iterate over tuple and apply function  */
using ForwardIterateTuple = IterateTuple<false>;

/** Reverse iterate over tuple and apply function */
using ReverseIterateTuple = IterateTuple<true>;

}  // namespace hermes_shm

#endif  // HERMES_INCLUDE_HERMES_DATA_STRUCTURES_TupleBase_H_
