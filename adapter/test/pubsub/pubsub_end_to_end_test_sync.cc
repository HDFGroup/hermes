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

#include <pubsub/pubsub.h>

int main(int argc, char **argv) {
  hermes::pubsub::mpiInit(argc, argv);
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  int comm_size;
  MPI_Comm_size(MPI_COMM_WORLD, &comm_size);

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }
  else{
    config_file = getenv(kHermesConf);
  }

  auto connect_ret = hermes::pubsub::connect(config_file);

  if(connect_ret.Succeeded()) {
    auto attach_ret = hermes::pubsub::attach("test");
    assert(attach_ret.Succeeded());

    std::vector<hapi::Blob> full_data;
    hapi::Blob data1(4*1024, rand() % 255);
    hapi::Blob data2(4*1024, rand() % 255);
    hapi::Blob data3(4*1024, rand() % 255);
    full_data.push_back(data1);
    full_data.push_back(data2);
    full_data.push_back(data3);

    for(const auto& data : full_data){
      auto publish_ret = hermes::pubsub::publish("test", data);
      assert(publish_ret.Succeeded());
    }

    unsigned long num_messages = full_data.size() * comm_size;
    std::pair<hapi::Blob, hapi::Status> subscribe_ret;
    for(unsigned long i = 0; i < num_messages; i++) {
      subscribe_ret = hermes::pubsub::subscribe("test");
      assert(subscribe_ret.second.Succeeded());
    }

    auto detach_ret = hermes::pubsub::detach("test");
    assert(detach_ret.Succeeded());
  }
  auto disconnect_ret = hermes::pubsub::disconnect();
  assert(disconnect_ret.Succeeded());
  MPI_Finalize();
}
