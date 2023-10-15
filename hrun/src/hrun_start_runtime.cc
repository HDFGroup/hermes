//
// Created by lukemartinlogan on 6/17/23.
//

#include "hermes_shm/util/singleton.h"
#include "hrun/api/hrun_runtime.h"

int main(int argc, char **argv) {
  HRUN_RUNTIME->Create();
  HRUN_RUNTIME->RunDaemon();
  HRUN_RUNTIME->Finalize();
  return 0;
}
