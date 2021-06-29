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

//#include <assert.h>

//#include <mpi.h>

#include <hermes/adapter/constants.h>

#include "hermes.h"
#include "mpi.h"
//#include "bucket.h"
//#include "test_utils.h"

namespace hapi = hermes::api;

void TestPutGetBucket(hapi::Bucket &bucket, int app_rank, int app_size) {
    size_t bytes_per_rank = KILOBYTES(8);
    if (app_size) {
        size_t data_size = KILOBYTES(8);
        bytes_per_rank = data_size / app_size;
        size_t remaining_bytes = data_size % app_size;

        if (app_rank == app_size - 1) {
            bytes_per_rank += remaining_bytes;
        }
    }

    hapi::Blob put_data(bytes_per_rank, rand() % 255);

    std::string blob_name = "test_blob" + std::to_string(app_rank);
    hermes::api::Status status = bucket.Put(blob_name, put_data);
    Assert(status.Succeeded());

    hapi::Blob get_result;
    size_t blob_size = bucket.Get(blob_name, get_result);
    get_result.resize(blob_size);
    blob_size = bucket.Get(blob_name, get_result);

    Assert(put_data == get_result);
}

void TestBulkTransfer(std::shared_ptr<hapi::Hermes> hermes, int app_rank) {
    size_t transfer_size = KILOBYTES(8);

    hapi::Bucket bucket(std::string("bulk_bucket"), hermes);
    std::string blob_name = "1";

    hapi::Blob put_data(transfer_size, 'x');

    if (app_rank == 0) {
        hermes::api::Status status = bucket.Put(blob_name, put_data);
        Assert(status.Succeeded());
    }

    hermes->AppBarrier();

    if (app_rank != 0) {
        hapi::Blob get_result;
        size_t blob_size = bucket.Get(blob_name, get_result);
        get_result.resize(blob_size);
        blob_size = bucket.Get(blob_name, get_result);

        Assert(get_result == put_data);

        bucket.Release();
    }

    hermes->AppBarrier();

    if (app_rank == 0) {
        bucket.Destroy();
    }
}

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

  auto connect_ret = hermes::pubsub::connect(config_file);
  assert(connect_ret.Succeeded());

  auto attach_ret = hermes::pubsub::attach("test");
  assert(attach_ret.Succeeded());

  unsigned char data1[6] = "test0";
  unsigned char data2[6] = "test1";
  unsigned char data3[6] = "test2";

  auto publish_ret = hermes::pubsub::publish("test", std::vector<unsigned char>(data1, data1 + 6));
  assert(publish_ret.Succeeded());

  auto subscribe_ret = hermes::pubsub::subscribe("test");
  assert(subscribe_ret.second.Succeeded());
  assert(memcmp(data1, &subscribe_ret.first.front(), 6));

  publish_ret = hermes::pubsub::publish("test", std::vector<unsigned char>(data2, data2 + 6));
  assert(publish_ret.Succeeded());

  publish_ret = hermes::pubsub::publish("test", std::vector<unsigned char>(data3, data3 + 6));
  assert(publish_ret.Succeeded());

  subscribe_ret = hermes::pubsub::subscribe("test");
  assert(subscribe_ret.second.Succeeded());
  assert(memcmp(data3, &subscribe_ret.first.front(), 6));

  auto detach_ret = hermes::pubsub::detach("test");
  assert(detach_ret.Succeeded());

  auto disconnect_ret = hermes::pubsub::disconnect();
  assert(disconnect_ret.Succeeded());
}
