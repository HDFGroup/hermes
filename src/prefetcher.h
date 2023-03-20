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

#ifndef HERMES_SRC_PREFETCHER_H_
#define HERMES_SRC_PREFETCHER_H_

#include "hermes_types.h"
#include "metadata_manager.h"
#include "rpc.h"
#include <list>

namespace hermes {

class Prefetcher {
 public:
  std::vector<std::list<IoTrace>> trace_;
  std::vector<std::list<IoTrace>::iterator> trace_off_;
  MetadataManager *mdm_;
  RPC_TYPE *rpc_;
  BufferOrganizer *borg_;
  tl::engine *engine;            /**< Argobots execution engine */
  ABT_xstream execution_stream_; /**< Argobots execution stream */
  double epoch_ms_;              /**< Milliseconds to sleep */

 public:
  /** Initialize each candidate prefetcher, including trace info */
  void Init();

  /** Parse the MDM's I/O pattern log */
  void Run();
};

}  // namespace hermes

#endif  // HERMES_SRC_PREFETCHER_H_
