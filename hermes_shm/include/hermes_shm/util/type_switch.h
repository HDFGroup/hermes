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

#ifndef HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_STATIC_SWITCH_H_
#define HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_STATIC_SWITCH_H_

#include  <functional>

namespace hshm {

/** Ends the recurrence of the switch-case */
class EndTypeSwitch {};

/**
 * A compile-time switch-case statement used for choosing
 * a type based on another type
 *
 * @param T the type being checked (i.e., switch (T)
 * @param Default the default case
 * @param Case a case of the switch (i.e., case Case:)
 * @param Val the body of the case
 * */
template<typename T, typename Default,
  typename Case = EndTypeSwitch,
  typename Val = EndTypeSwitch,
  typename ...Args>
struct type_switch {
  typedef typename std::conditional<
    std::is_same_v<T, Case>,
    Val,
    typename type_switch<T, Default, Args...>::type>::type type;
};

/** The default case */
template<typename T, typename Default>
struct type_switch<T, Default, EndTypeSwitch, EndTypeSwitch> {
  typedef Default type;
};

}  // namespace hshm

#endif  // HERMES_SHM_INCLUDE_HERMES_SHM_UTIL_STATIC_SWITCH_H_
