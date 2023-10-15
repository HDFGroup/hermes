//
// Created by lukemartinlogan on 7/5/23.
//

#ifndef HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_AFFINITY_H_
#define HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_AFFINITY_H_

#include <sched.h>

class ProcessAffiner {
 public:
  /** Set the CPU affinity of a process */
  static void SetCpuAffinity(int pid, int cpu_id) {
    // Create a CPU set and set CPU affinity to CPU 0
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu_id, &cpuset);

    // Set the CPU affinity of the process
    int result = sched_setaffinity(pid, sizeof(cpuset), &cpuset);
    if (result == -1) {
      // HELOG(kError, "Failed to set CPU affinity for process {}", pid);
    }
  }
};

#endif  // HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_AFFINITY_H_
