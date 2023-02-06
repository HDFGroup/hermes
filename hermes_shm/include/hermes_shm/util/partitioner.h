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

#ifndef HERMES_SHM_PARTITIONER_H
#define HERMES_SHM_PARTITIONER_H

// Reference: https://stackoverflow.com/questions/63372288/getting-list-of-pids-from-proc-in-linux

#include <vector>
#include <dirent.h>
#include <sys/sysinfo.h>
#include <sched.h>
#include <string>

namespace hermes_shm {

class ProcessAffiner {
 private:
  int n_cpu_;
  cpu_set_t *cpus_;

 public:
  ProcessAffiner() {
    n_cpu_ = get_nprocs_conf();
    cpus_ = new cpu_set_t[n_cpu_];
    CPU_ZERO(cpus_);
  }

  ~ProcessAffiner() {
    delete cpus_;
  }

  inline bool isdigit(char digit) {
    return ('0' <= digit && digit <= '9');
  }

  inline int GetNumCPU() {
    return n_cpu_;
  }

  inline void SetCpu(int cpu) {
    CPU_SET(cpu, cpus_);
  }

  inline void SetCpus(int off, int len) {
    for (int i = 0; i < len; ++i) {
      SetCpu(off + i);
    }
  }

  inline void ClearCpu(int cpu) {
    CPU_CLR(cpu, cpus_);
  }

  inline void ClearCpus(int off, int len) {
    for (int i = 0; i < len; ++i) {
      ClearCpu(off + i);
    }
  }

  inline void Clear() {
    CPU_ZERO(cpus_);
  }

  int AffineAll(void) {
    DIR *procdir;
    struct dirent *entry;
    int count = 0;

    // Open /proc directory.
    procdir = opendir("/proc");
    if (!procdir) {
      perror("opendir failed");
      return 0;
    }

    // Iterate through all files and folders of /proc.
    while ((entry = readdir(procdir))) {
      // Skip anything that is not a PID folder.
      if (!is_pid_folder(entry))
        continue;
      // Get the PID of the running process
      int proc_pid = atoi(entry->d_name);
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
    int count = 0;
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

}  // namespace hermes_shm

#endif  // HERMES_SHM_PARTITIONER_H
