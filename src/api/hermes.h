//
// Created by lukemartinlogan on 12/2/22.
//

#ifndef HERMES_SRC_API_HERMES_H_
#define HERMES_SRC_API_HERMES_H_

#include "config_client.h"
#include "config_server.h"
#include "constants.h"
#include "hermes_types.h"

#include "communication_factory.h"
#include "rpc.h"
#include "metadata_manager.h"
#include "buffer_pool.h"
#include "buffer_organizer.h"

namespace hermes::api {

class Bucket;
class VBucket;


/**
 * The Hermes shared-memory header
 * */
struct HermesShmHeader {
  MetadataManagerShmHeader mdm_;
  BufferPoolShmHeader bpm_;
};

/**
 * An index into all Hermes-related data structures.
 * */
class Hermes {
 public:
  HermesType mode_;
  HermesShmHeader *header_;
  ServerConfig server_config_;
  ClientConfig client_config_;
  MetadataManager mdm_;
  BufferPool bpm_;
  BufferOrganizer borg_;
  COMM_TYPE comm_;
  RPC_TYPE rpc_;
  lipc::Allocator *main_alloc_;

 public:
  Hermes() = default;

  static Hermes* Create(HermesType mode = HermesType::kClient,
                        std::string server_config_path = "",
                        std::string client_config_path = "") {
    auto hermes = HERMES;
    hermes->Init(mode, server_config_path, client_config_path);
    return hermes;
  }

  void Init(HermesType mode = HermesType::kClient,
            std::string server_config_path = "",
            std::string client_config_path = "");

  void Finalize();

  void RunDaemon();

  void StopDaemon();

 public:
  std::shared_ptr<Bucket> GetBucket(std::string name,
                                    Context ctx = Context());
  std::shared_ptr<VBucket> GetVBucket(std::string name,
                                      Context ctx = Context());

 private:
  void InitServer(std::string server_config_path);

  void InitColocated(std::string server_config_path,
                     std::string client_config_path);

  void InitClient(std::string server_config_path,
                  std::string client_config_path);

  void LoadServerConfig(std::string config_path);

  void LoadClientConfig(std::string config_path);

  void InitSharedMemory();

  void LoadSharedMemory();

  void FinalizeServer();

  void FinalizeClient();

  void FinalizeColocated();

 private:
  inline std::string GetEnvSafe(const char *env_name) {
    char *val = getenv(env_name);
    if (val == nullptr){
      return "";
    }
    return val;
  }
};

}  // namespace hermes::api

#endif  // HERMES_SRC_API_HERMES_H_
