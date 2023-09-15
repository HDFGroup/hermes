//
// Created by lukemartinlogan on 6/17/23.
//

#include "hermes_shm/util/singleton.h"
#include "labstor/api/labstor_runtime.h"

int main(int argc, char **argv) {
  LABSTOR_RUNTIME->Create();
  LABSTOR_RUNTIME->RunDaemon();
  LABSTOR_RUNTIME->Finalize();
  return 0;
}
