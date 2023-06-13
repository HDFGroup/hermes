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

#ifndef HERMES_THREAD_THREAD_FACTORY_H_
#define HERMES_THREAD_THREAD_FACTORY_H_

#include "thread_model.h"

namespace hshm::thread_model {

class ThreadFactory {
 public:
  /** Get a thread instance */
  static std::unique_ptr<ThreadModel> Get(ThreadType type);
};

}  // namespace hshm::thread_model

#endif  // HERMES_THREAD_THREAD_FACTORY_H_
