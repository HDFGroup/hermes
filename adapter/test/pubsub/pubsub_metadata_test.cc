//
// Created by jaime on 6/25/2021.
//
#include <hermes/adapter/pubsub/metadata_manager.h>
#include <hermes/adapter/singleton.h>

int main(int argc, char **argv) {
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  ClientMetadata stat;
  struct timespec ts{};
  timespec_get(&ts, TIME_UTC);
  stat.st_atim = ts;

  auto create_ret = mdm->Create("test", stat);
  assert(create_ret == true);
  create_ret = mdm->Create("test", stat);
  assert(create_ret == false);

  auto find_ret = mdm->Find("test");
  assert(find_ret.second == true);
  assert(find_ret.first.st_atim.tv_nsec == ts.tv_nsec);

  struct timespec ts1{};
  timespec_get(&ts1, TIME_UTC);
  stat.st_atim = ts1;
  auto update_ret = mdm->Update("test", stat);
  assert(update_ret == true);
  find_ret = mdm->Find("test");
  assert(find_ret.second == true);
  assert(find_ret.first.st_atim.tv_nsec <= ts.tv_nsec);
  assert(find_ret.first.st_atim.tv_nsec == ts1.tv_nsec);

  auto delete_ret = mdm->Delete("test");
  assert(delete_ret == true);
  delete_ret = mdm->Delete("test");
  assert(delete_ret == false);
  find_ret = mdm->Find("test");
  assert(find_ret.second == false);
  update_ret = mdm->Update("test", stat);
  assert(update_ret == false);
}