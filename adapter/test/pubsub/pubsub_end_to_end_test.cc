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
#include "test_utils.h"

int main(int argc, char **argv) {
  hermes::pubsub::mpiInit(argc, argv);

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  } else {
    config_file = getenv(kHermesConf);
  }

  auto connect_ret = hermes::pubsub::connect(config_file);
  Assert(connect_ret.Succeeded());

  if (connect_ret.Succeeded()) {
    auto attach_ret = hermes::pubsub::attach("test");
    Assert(attach_ret.Succeeded());

    hapi::Blob data1(4*1024, rand() % 255);
    hapi::Blob data2(4*1024, rand() % 255);
    hapi::Blob data3(4*1024, rand() % 255);

    auto publish_ret = hermes::pubsub::publish("test", data1);
    Assert(publish_ret.Succeeded());

    auto subscribe_ret_1 = hermes::pubsub::subscribe("test");
    Assert(subscribe_ret_1.second.Succeeded());
    Assert(data1 == subscribe_ret_1.first);

    publish_ret = hermes::pubsub::publish("test", data2);
    Assert(publish_ret.Succeeded());

    publish_ret = hermes::pubsub::publish("test", data3);
    Assert(publish_ret.Succeeded());

    // this subscribe reads data2;
    hermes::pubsub::subscribe("test");

    auto subscribe_ret_2 = hermes::pubsub::subscribe("test");
    Assert(subscribe_ret_2.second.Succeeded());
    Assert(data3 == subscribe_ret_2.first);

    auto detach_ret = hermes::pubsub::detach("test");
    Assert(detach_ret.Succeeded());
  }
  auto disconnect_ret = hermes::pubsub::disconnect();
  Assert(disconnect_ret.Succeeded());

  MPI_Finalize();
}
