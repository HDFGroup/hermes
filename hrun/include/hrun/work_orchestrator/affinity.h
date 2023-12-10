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

#ifndef HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_AFFINITY_H_
#define HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_AFFINITY_H_

#include <sched.h>
#include <vector>
#include <dirent.h>
#include <sys/sysinfo.h>
#include <sched.h>
#include <string>

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

 private:
  int n_cpu_;
  std::vector<cpu_set_t> cpus_;
  std::vector<int> ignore_pids_;

 public:
  ProcessAffiner() {
    n_cpu_ = get_nprocs_conf();
    cpus_.resize(n_cpu_);
    Clear();
  }

  inline bool isdigit(char digit) {
    return ('0' <= digit && digit <= '9');
  }

  inline int GetNumCPU() {
    return n_cpu_;
  }

  inline void SetCpu(int cpu) {
    CPU_SET(cpu, cpus_.data());
  }

  inline void SetCpus(int off, int len) {
    for (int i = 0; i < len; ++i) {
      SetCpu(off + i);
    }
  }

  inline void SetCpus(const std::vector<int> &cpu_ids) {
    for (int cpu_id : cpu_ids) {
      SetCpu(cpu_id);
    }
  }

  inline void ClearCpu(int cpu) {
    CPU_CLR(cpu, cpus_.data());
  }

  void IgnorePids(const std::vector<int> &pids) {
    ignore_pids_ = pids;
  }

  inline void ClearCpus(int off, int len) {
    for (int i = 0; i < len; ++i) {
      ClearCpu(off + i);
    }
  }

  inline void Clear() {
    for (cpu_set_t &cpu : cpus_) {
      CPU_ZERO(&cpu);
    }
  }

  int AffineAll(void) {
    DIR *procdir;
    struct dirent *entry;
    size_t count = 0;

    // Open /proc directory.
    procdir = opendir("/proc");
    if (!procdir) {
      perror("opendir failed");
      return 0;
    }

    // Iterate through all files and folders of /proc.
    while ((entry = readdir(procdir))) {
      // Skip anything that is not a PID folder.
      if (!is_pid_folder(entry)) {
        continue;
      }
      // Get the PID of the running process
      int proc_pid = atoi(entry->d_name);
      if (std::find(ignore_pids_.begin(), ignore_pids_.end(), proc_pid) !=
          ignore_pids_.end()) {
        continue;
      }
      // Set the affinity of all running process to this mask
      count += Affine(proc_pid);
    }
    closedir(procdir);
    return count;
  }
  int Affine(std::vector<pid_t> &&pids) {
    return Affine(pids);
  }
  int Affine(std::vector<pid_t> &pids) {
    // Set the affinity of all running process to this mask
    size_t count = 0;
    for (pid_t &pid : pids) {
      count += Affine(pid);
    }
    return count;
  }
  int Affine(int pid) {
    return SetAffinitySafe(pid, n_cpu_, cpus_);
  }

  void PrintAffinity(int pid) {
    PrintAffinity("", pid);
  }
  void PrintAffinity(std::string prefix, int pid) {
    std::vector<cpu_set_t> cpus(n_cpu_);
    sched_getaffinity(pid, n_cpu_, cpus.data());
    PrintAffinity(prefix, pid, cpus.data());
  }

  void PrintAffinity(std::string prefix, int pid, cpu_set_t *cpus) {
    std::string affinity = "";
    for (int i = 0; i < n_cpu_; ++i) {
      if (CPU_ISSET(i, cpus)) {
        affinity += std::to_string(i) + ", ";
      }
    }
    printf("%s: CPU affinity[pid=%d]: %s\n", prefix.c_str(),
           pid, affinity.c_str());
  }

 private:
  int SetAffinitySafe(int pid, int n_cpu, cpu_set_t *cpus) {
    int ret = sched_setaffinity(pid, n_cpu, cpus);
    if (ret == -1) {
      return 0;
    }
    return 1;
  }

  // Helper function to check if a struct dirent from /proc is a PID folder.
  int is_pid_folder(const struct dirent *entry) {
    const char *p;
    for (p = entry->d_name; *p; p++) {
      if (!isdigit(*p))
        return false;
    }
    return true;
  }
};

#endif  // HRUN_INCLUDE_HRUN_WORK_ORCHESTRATOR_AFFINITY_H_
