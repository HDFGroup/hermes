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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_ARGPACK_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_ARGPACK_H_

#include "basic.h"
#include  <functional>

namespace hermes_shm {

/** Type which indicates that a constructor takes ArgPacks as input */
struct PiecewiseConstruct {};

/** Type which ends template recurrence */
struct EndTemplateRecurrence {};

/** Recurrence used to create argument pack */
template<
  size_t idx,
  typename T=EndTemplateRecurrence,
  typename ...Args>
struct ArgPackRecur {
  constexpr static bool is_rval = std::is_rvalue_reference<T>();
  typedef typename std::conditional<is_rval, T&&, T&>::type ElementT;

  ElementT arg_; /**< The element stored */
  ArgPackRecur<idx + 1, Args...>
    recur_; /**< Remaining args */

  /** Default constructor */
  ArgPackRecur() = default;

  /** Constructor. Rvalue reference. */
  explicit ArgPackRecur(T arg, Args&& ...args)
  : arg_(std::forward<T>(arg)), recur_(std::forward<Args>(args)...) {}

  /** Forward an rvalue reference (only if argpack) */
  template<size_t i>
  constexpr decltype(auto) Forward() const {
    if constexpr(i == idx) {
      if constexpr(is_rval) {
        return std::forward<T>(arg_);
      } else {
        return arg_;
      }
    } else {
      return recur_.template
        Forward<i>();
    }
  }
};

/** Terminator of the ArgPack recurrence */
template<size_t idx>
struct ArgPackRecur<idx, EndTemplateRecurrence> {
  /** Default constructor */
  ArgPackRecur() = default;

  /** Forward an rvalue reference (only if argpack) */
  template<size_t i>
  constexpr void Forward() const {
    throw std::logic_error("(Forward) ArgPack index outside of range");
  }
};

/** Used to semantically pack arguments */
template<typename ...Args>
struct ArgPack {
  /** Variable argument pack */
  ArgPackRecur<0, Args...> recur_;
  /** Size of the argpack */
  constexpr const static size_t size_ = sizeof...(Args);

  /** General Constructor. */
  ArgPack(Args&& ...args)
  : recur_(std::forward<Args>(args)...) {}

  /** Get forward reference */
  template<size_t idx>
  constexpr decltype(auto) Forward() const {
    return recur_.template Forward<idx>();
  }

  /** Size */
  constexpr static size_t Size() {
    return size_;
  }
};

/** Make an argpack */
template<typename ...Args>
ArgPack<Args&&...> make_argpack(Args&& ...args) {
  return ArgPack<Args&&...>(std::forward<Args>(args)...);
}

/** Get the type + reference of the forward for \a pack pack at \a index i */
#define FORWARD_ARGPACK_FULL_TYPE(pack, i)\
  decltype(pack.template Forward<i>())

/** Get type of the forward for \a pack pack at \a index i */
#define FORWARD_ARGPACK_BASE_TYPE(pack, i)\
  std::remove_reference<FORWARD_ARGPACK_FULL_TYPE(pack, i)>

/** Forward the param for \a pack pack at \a index i */
#define FORWARD_ARGPACK_PARAM(pack, i)\
  std::forward<FORWARD_ARGPACK_FULL_TYPE(pack, i)>(\
    pack.template Forward<i>())

/** Used to pass an argument pack to a function or class method */
class PassArgPack {
 public:
  /** Call function with ArgPack */
  template<typename ArgPackT, typename F>
  constexpr static decltype(auto) Call(ArgPackT &&pack, F &&f) {
    return _CallRecur<0, ArgPackT, F>(
      std::forward<ArgPackT>(pack), std::forward<F>(f));
  }

 private:
  /** Unpacks the ArgPack and passes it to the function */
  template<size_t i, typename ArgPackT,
    typename F, typename ...CurArgs>
  constexpr static decltype(auto) _CallRecur(ArgPackT &&pack,
                                             F &&f,
                                             CurArgs&& ...args) {
    if constexpr(i < ArgPackT::Size()) {
      return _CallRecur<i + 1, ArgPackT, F>(
        std::forward<ArgPackT>(pack),
        std::forward<F>(f),
        std::forward<CurArgs>(args)...,
        FORWARD_ARGPACK_PARAM(pack, i));
    } else {
      return std::__invoke(f, std::forward<CurArgs>(args)...);
    }
  }
};

/** Combine multiple argpacks into a single argpack */
class MergeArgPacks {
 public:
  /** Call function with ArgPack */
  template<typename ...ArgPacks>
  constexpr static decltype(auto) Merge(ArgPacks&& ...packs) {
    return _MergePacksRecur<0>(make_argpack(std::forward<ArgPacks>(packs)...));
  }

 private:
  /** Unpacks the C++ parameter pack of ArgPacks */
  template<size_t cur_pack, typename ArgPacksT,
    typename ...CurArgs>
  constexpr static decltype(auto) _MergePacksRecur(ArgPacksT &&packs,
                                         CurArgs&& ...args) {
    if constexpr(cur_pack < ArgPacksT::Size()) {
      return _MergeRecur<
        cur_pack, ArgPacksT, 0>(
        // End template parameters
        std::forward<ArgPacksT>(packs),
        FORWARD_ARGPACK_PARAM(packs, cur_pack),
        std::forward<CurArgs>(args)...
      );
    } else {
      return make_argpack(std::forward<CurArgs>(args)...);
    }
  }

  /** Unpacks the C++ parameter pack of ArgPacks */
  template<
    size_t cur_pack, typename ArgPacksT,
    size_t i, typename ArgPackT,
    typename ...CurArgs>
  constexpr static decltype(auto) _MergeRecur(ArgPacksT &&packs,
                                    ArgPackT &&pack,
                                    CurArgs&& ...args) {
    if constexpr(i < ArgPackT::Size()) {
      return _MergeRecur<cur_pack, ArgPacksT, i + 1, ArgPackT>(
        std::forward<ArgPacksT>(packs),
        std::forward<ArgPackT>(pack),
        std::forward<CurArgs>(args)..., FORWARD_ARGPACK_PARAM(pack, i));
    } else {
      return _MergePacksRecur<cur_pack + 1, ArgPacksT>(
        std::forward<ArgPacksT>(packs),
        std::forward<CurArgs>(args)...);
    }
  }
};

/** Insert an argpack at the head of each pack in a set of ArgPacks */
class ProductArgPacks {
 public:
  /** The product function */
  template<typename ProductPackT, typename ...ArgPacks>
  constexpr static decltype(auto) Product(ProductPackT &&prod_pack,
                                ArgPacks&& ...packs) {
    return _ProductPacksRecur<0>(
      std::forward<ProductPackT>(prod_pack),
      make_argpack(std::forward<ArgPacks>(packs)...));
  }

 private:
  /** Prepend \a ArgPack prod_pack to every ArgPack in orig_packs */
  template<
    size_t cur_pack,
    typename ProductPackT,
    typename OrigPacksT,
    typename ...NewPacks>
  constexpr static decltype(auto) _ProductPacksRecur(ProductPackT &&prod_pack,
                                           OrigPacksT &&orig_packs,
                                           NewPacks&& ...packs) {
    if constexpr(cur_pack < OrigPacksT::Size()) {
      return _ProductPacksRecur<cur_pack + 1>(
        std::forward<ProductPackT>(prod_pack),
        std::forward<OrigPacksT>(orig_packs),
        std::forward<NewPacks>(packs)...,
        std::forward<ProductPackT>(prod_pack),
        FORWARD_ARGPACK_PARAM(orig_packs, cur_pack));
    } else {
      return make_argpack(std::forward<NewPacks>(packs)...);
    }
  }
};

}  // namespace hermes_shm

#endif //HERMES_SHM_INCLUDE_HERMES_SHM_TYPES_ARGPACK_H_
