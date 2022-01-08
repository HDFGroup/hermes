//
// Created by jaime on 6/3/2021.
//
#include "hermes/adapter/pubsub.h"

namespace hapi = hapi;

hapi::Status hermes::pubsub::mpiInit(int argc, char **argv){
  LOG(INFO) << "Starting MPI" << std::endl;
  int mpi_threads_provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &mpi_threads_provided);
  if (mpi_threads_provided < MPI_THREAD_MULTIPLE) {
    fprintf(stderr, "Didn't receive appropriate MPI threading specification\n");
    return hapi::Status(hermes::INVALID_FILE);
  }
  return hapi::Status(hermes::HERMES_SUCCESS);
}

hapi::Status hermes::pubsub::connect(const std::string &config_file){
  LOG(INFO) << "Connecting adapter" << std::endl;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  try {
    mdm->InitializeHermes(config_file.c_str());
    if(mdm->isClient()) return hapi::Status(hermes::HERMES_SUCCESS);
    else return hapi::Status(hermes::INVALID_FILE);
  } catch (const std::exception& e){
    LOG(FATAL) << "Could not connect to hermes daemon" <<std::endl;
    return hapi::Status(hermes::INVALID_FILE);
  }
}

hapi::Status hermes::pubsub::connect(){
  LOG(INFO) << "Connecting adapter" << std::endl;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  char* hermes_config = getenv(kHermesConf);
  try {
    mdm->InitializeHermes(hermes_config);
    if(mdm->isClient()) return hapi::Status(hermes::HERMES_SUCCESS);
    else return hapi::Status(hermes::INVALID_FILE);
  } catch (const std::exception& e){
    LOG(FATAL) << "Could not connect to hermes daemon" <<std::endl;
    return hapi::Status(hermes::INVALID_FILE);
  }
}

hapi::Status hermes::pubsub::disconnect(){
  LOG(INFO) << "Disconnecting adapter" << std::endl;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  try {
    mdm->FinalizeHermes();
    return hapi::Status(hermes::HERMES_SUCCESS);
  } catch (const std::exception& e){
    LOG(FATAL) << "Could not disconnect from hermes" <<std::endl;
    return hapi::Status(hermes::INVALID_FILE);
  }

}

hapi::Status hermes::pubsub::attach(const std::string& topic){
  LOG(INFO) << "Attaching to topic: " << topic << std::endl;
  hapi::Context ctx;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  auto existing = mdm->Find(topic);
  if (!existing.second) {
    LOG(INFO) << "Topic not existing" << std::endl;
    ClientMetadata stat;
//    stat.ref_count = 1;
    struct timespec ts{};
    timespec_get(&ts, TIME_UTC);
    stat.st_atim = ts;
    stat.st_bkid = std::make_shared<hapi::Bucket>(topic, mdm->GetHermes(), ctx);
    if(!stat.st_bkid->IsValid()) return hapi::Status(hermes::INVALID_BUCKET);
    if(!mdm->Create(topic, stat)) return hapi::Status(hermes::INVALID_BUCKET);
  }
  else{
    LOG(INFO) << "File exists" << std::endl;
//    existing.first.ref_count++;
    struct timespec ts{};
    timespec_get(&ts, TIME_UTC);
    existing.first.st_atim = ts;
    if(!mdm->Update(topic, existing.first)) return hapi::Status(hermes::INVALID_BUCKET);
  }
  return hapi::Status(hermes::HERMES_SUCCESS);
}

hapi::Status hermes::pubsub::detach(const std::string& topic){
  LOG(INFO) << "Detaching from topic: " << topic << std::endl;
  hapi::Context ctx;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  auto existing = mdm->Find(topic);
  if (existing.second) {
    mdm->Delete(topic);
    return existing.first.st_bkid->Release(ctx);
  }
  return hapi::Status(hermes::INVALID_BUCKET);
}

hapi::Status hermes::pubsub::publish(const std::string& topic, const std::vector<unsigned char>& message){
  LOG(INFO) << "Publish to : " << topic << std::endl;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  auto metadata = mdm->Find(topic);

  if(!metadata.second){
    if(attach(topic) == hermes::HERMES_SUCCESS) metadata = mdm->Find(topic);
    else return hapi::Status(hermes::INVALID_BUCKET);
  }

  hapi::Context ctx;

  struct timespec ts{};
  timespec_get(&ts, TIME_UTC);

  //TODO(Jaime): We might want to add some considerations for threads.
  //Currently i am deploying 40 process per node no threads used.
  std::string blob_name = std::to_string(mdm->mpi_rank) + "_" +
                          std::to_string(ts.tv_nsec);
  LOG(INFO) << "Publishing to blob with id " << blob_name << std::endl;
  auto status = metadata.first.st_bkid->Put(blob_name, message, ctx);
  if (status.Failed()) {
    return hapi::Status(hermes::INVALID_BLOB);
  }

  metadata.first.st_atim = ts;
  mdm->Update(topic, metadata.first);

  return hapi::Status(hermes::HERMES_SUCCESS);
}

std::pair<std::vector<unsigned char>, hapi::Status> hermes::pubsub::subscribe(const std::string& topic){
  LOG(INFO) << "Publish to : " << topic << std::endl;
  typedef std::pair<std::vector<unsigned char>, hapi::Status> SubscribeReturn;

  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  auto metadata = mdm->Find(topic);

  if(!metadata.second){
    if(attach(topic) == hermes::HERMES_SUCCESS) metadata = mdm->Find(topic);
    else return SubscribeReturn(std::vector<unsigned char>(), 
        hapi::Status(hermes::INVALID_BUCKET));
  }

  hapi::Context ctx;
  u64 index = metadata.first.last_subscribed_blob;

  hapi::Blob read_data(0);
  LOG(INFO) << "Subscribing to  " << topic << " at blob id " << index
            << std::endl;
  auto exiting_blob_size =
      metadata.first.st_bkid->GetNext(index, read_data, ctx);
  read_data.resize(exiting_blob_size);
  if(metadata.first.st_bkid->GetNext(index, read_data, ctx) != exiting_blob_size){
    return SubscribeReturn(std::vector<unsigned char>(),
        hapi::Status(hermes::BLOB_NOT_IN_BUCKET));
  }

  metadata.first.last_subscribed_blob++;
  struct timespec ts{};
  timespec_get(&ts, TIME_UTC);
  metadata.first.st_atim = ts;
  mdm->Update(topic, metadata.first);

  //TODO: Maybe stablish a memecopy, or discuss proper return types
  //Issue here is the trasnlation between std::vector<unsigned char> and string
  //I am trasnforming the string into an unisgned char* on put, and getting a
  // unisgned char* in here

  return SubscribeReturn(read_data,
                         hapi::Status(hermes::HERMES_SUCCESS));
}