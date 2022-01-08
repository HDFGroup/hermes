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

#include <pubsub.h>

int main(int argc, char **argv) {
  hermes::pubsub::mpiInit(argc, argv);

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }
  else{
    config_file = getenv(kHermesConf);
  }

  auto connect_ret = hermes::pubsub::connect(config_file);
  assert(connect_ret.Succeeded());

  if(connect_ret.Succeeded()) {
    auto attach_ret = hermes::pubsub::attach("test");
    assert(attach_ret.Succeeded());

    unsigned char data1[6] = "test0";
    unsigned char data2[6] = "test1";
    unsigned char data3[6] = "test2";

    auto publish_ret = hermes::pubsub::publish(
        "test", std::vector<unsigned char>(data1, data1 + 6));
    assert(publish_ret.Succeeded());

    auto subscribe_ret = hermes::pubsub::subscribe("test");
    assert(subscribe_ret.second.Succeeded());
    assert(memcmp(data1, &subscribe_ret.first.front(), 4));

    publish_ret = hermes::pubsub::publish(
        "test", std::vector<unsigned char>(data2, data2 + 6));
    assert(publish_ret.Succeeded());

    publish_ret = hermes::pubsub::publish(
        "test", std::vector<unsigned char>(data3, data3 + 6));
    assert(publish_ret.Succeeded());

    subscribe_ret = hermes::pubsub::subscribe("test");
    assert(subscribe_ret.second.Succeeded());
    assert(memcmp(data3, &subscribe_ret.first.front(), 4));

    auto detach_ret = hermes::pubsub::detach("test");
    assert(detach_ret.Succeeded());
  }
  auto disconnect_ret = hermes::pubsub::disconnect();
  assert(disconnect_ret.Succeeded());
}
