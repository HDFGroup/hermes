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

#include "sequential.h"

namespace hermes {

void SequentialPrefetcher::Process(std::list<IoLogEntry> &log,
                                   PrefetchSchema &schema) {
  // Group by tid
  std::unordered_map<GlobalThreadID, std::list<IoLogEntry>> per_thread_log;
  for (auto &entry : log) {
    per_thread_log[entry.tid_].emplace_back(entry);
  }

  // Estimate the time until next access

}

}  // namespace hermes