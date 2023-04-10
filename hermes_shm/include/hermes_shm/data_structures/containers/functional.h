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

#ifndef HERMES_SHM_SHM_DATA_STRUCTURES_CONTAINERS_FUNCTIONAL_H_
#define HERMES_SHM_SHM_DATA_STRUCTURES_CONTAINERS_FUNCTIONAL_H_

namespace hshm {

template<typename T, typename IteratorT>
IteratorT find(IteratorT start, const IteratorT &end, T &val) {
  for (; start != end; ++start) {
    T &ref = *start;
    if (ref == val) {
      return start;
    }
  }
  return end;
}

}  // namespace hshm

#endif  // HERMES_SHM_SHM_DATA_STRUCTURES_CONTAINERS_FUNCTIONAL_H_
