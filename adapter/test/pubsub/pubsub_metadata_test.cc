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

#include <pubsub/metadata_manager.h>
#include <singleton.h>
#include "test_utils.h"

int main() {
  auto mdm = hermes::adapter::Singleton
      <hermes::adapter::pubsub::MetadataManager>::GetInstance(false);
  ClientMetadata stat;
  struct timespec ts{};
  timespec_get(&ts, TIME_UTC);
  stat.st_atim = ts;

  auto create_ret = mdm->Create("test", stat);
  Assert(create_ret == true);
  create_ret = mdm->Create("test", stat);
  Assert(create_ret == false);

  auto find_ret = mdm->Find("test");
  Assert(find_ret.second == true);
  Assert(find_ret.first.st_atim.tv_nsec == ts.tv_nsec);

  struct timespec ts1{};
  timespec_get(&ts1, TIME_UTC);
  stat.st_atim = ts1;
  auto update_ret = mdm->Update("test", stat);
  Assert(update_ret == true);
  find_ret = mdm->Find("test");
  Assert(find_ret.second == true);
  Assert(find_ret.first.st_atim.tv_nsec >= ts.tv_nsec);
  Assert(find_ret.first.st_atim.tv_nsec == ts1.tv_nsec);

  auto delete_ret = mdm->Delete("test");
  Assert(delete_ret == true);
  delete_ret = mdm->Delete("test");
  Assert(delete_ret == false);
  find_ret = mdm->Find("test");
  Assert(find_ret.second == false);
  update_ret = mdm->Update("test", stat);
  Assert(update_ret == false);
}
