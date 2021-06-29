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
    return hapi::Status(hermes::HERMES_ERROR_MAX);
  }
  return hapi::Status(hermes::HERMES_SUCCESS);
}

hapi::Status hermes::pubsub::connect(const char *config_file){
  LOG(INFO) << "Connecting adapter" << std::endl;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  mdm->InitializeHermes(config_file);
  if(mdm->isClient()) return hapi::Status(hermes::HERMES_SUCCESS);
  else return hapi::Status(hermes::HERMES_ERROR_MAX);
}

hapi::Status hermes::pubsub::connect(){
  LOG(INFO) << "Connecting adapter" << std::endl;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  char* hermes_config = getenv(kHermesConf);
  mdm->InitializeHermes(hermes_config);
  if(mdm->isClient()) return hapi::Status(hermes::HERMES_SUCCESS);
  else return hapi::Status(hermes::HERMES_ERROR_MAX);
}

hapi::Status hermes::pubsub::disconnect(){
  LOG(INFO) << "Disconnecting adapter" << std::endl;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  mdm->FinalizeHermes();
  return hapi::Status(hermes::HERMES_SUCCESS);
}

hapi::Status hermes::pubsub::attach(const std::string& topic){
  LOG(INFO) << "Attaching to topic: " << topic << std::endl;
  hapi::Context ctx;
  auto mdm = hermes::adapter::Singleton<hermes::adapter::pubsub::MetadataManager>::GetInstance();
  auto existing = mdm->Find(topic);
  if (!existing.second) {
    LOG(INFO) << "Topic not existing" << std::endl;
    TopicMetadata stat;
    stat.ref_count = 1;
    struct timespec ts{};
    timespec_get(&ts, TIME_UTC);
    stat.st_atim = ts;
    stat.st_bkid = std::make_shared<hapi::Bucket>(topic, mdm->GetHermes(), ctx);
    if(!stat.st_bkid->IsValid()) return hapi::Status(hermes::INVALID_BUCKET);
    if(!mdm->Create(topic, stat)) return hapi::Status(hermes::INVALID_BUCKET);
  }
  else{
    LOG(INFO) << "File exists" << std::endl;
    existing.first.ref_count++;
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
  if (!existing.second) {
    if (existing.first.ref_count == 1) {
      mdm->Delete(topic);
      //TODO(Jaime): Do we need to clean the blobs
      return existing.first.st_bkid->Destroy(ctx);
    }
    else{
      LOG(INFO) << "Detaching from topic with more than one reference: "
                << topic << std::endl;
      existing.first.ref_count--;
      struct timespec ts{};
      timespec_get(&ts, TIME_UTC);
      existing.first.st_atim = ts;
      mdm->Update(topic, existing.first);
      return existing.first.st_bkid->Release(ctx);
    }
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

  auto index = std::to_string(metadata.first.final_blob);
  auto blob_exists = metadata.first.st_bkid->ContainsBlob(index);
  while(blob_exists){
    metadata.first.final_blob++;
    index = std::to_string(metadata.first.final_blob);
    blob_exists = metadata.first.st_bkid->ContainsBlob(index);
  }

  hapi::Context ctx;

  LOG(INFO) << "Publishing to blob with id " <<
      metadata.first.final_blob << std::endl;
  auto status = metadata.first.st_bkid->Put(index, message, ctx);
  if (status.Failed()) {
    return hapi::Status(hermes::INVALID_BLOB); //TODO (jaime): add error codes
  }

  metadata.first.final_blob++;
  struct timespec ts{};
  timespec_get(&ts, TIME_UTC);
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
  auto index = std::to_string(metadata.first.current_blob);
  auto blob_exists = metadata.first.st_bkid->ContainsBlob(index);

  hapi::Blob read_data(0);
  if (blob_exists) {
    LOG(INFO) << "Subscribing to  " << topic << " at blob id " << index
              << std::endl;

    auto exiting_blob_size =
        metadata.first.st_bkid->Get(index, read_data, ctx);
    read_data.resize(exiting_blob_size);

    if(metadata.first.st_bkid->Get(index, read_data, ctx) != exiting_blob_size){
      return SubscribeReturn(std::vector<unsigned char>(), 
          hapi::Status(hermes::BLOB_NOT_IN_BUCKET));
    }
  }
  else{
    LOG(INFO)
        << "Blob does not exists, catch up to tail or error at topic:"
        << topic << std::endl;
      return SubscribeReturn(std::vector<unsigned char>(), 
          hapi::Status(hermes::BLOB_NOT_IN_BUCKET));
  }

  metadata.first.current_blob++;
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