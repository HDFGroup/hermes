//
// Created by jaime on 6/25/2021.
//

#include <hermes/adapter/pubsub.h>
int main(int argc, char **argv) {
  hermes::pubsub::mpiInit(argc, argv);

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }
  else{
    config_file = getenv(kHermesConf);
  }

  auto connect_ret = hermes::pubsub::connect(false);
  assert(connect_ret == hermes::INVALID_FILE);
  auto disconnect_ret = hermes::pubsub::disconnect();
  assert(disconnect_ret == hermes::INVALID_FILE);
}