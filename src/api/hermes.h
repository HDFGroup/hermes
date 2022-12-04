//
// Created by lukemartinlogan on 12/2/22.
//

#ifndef HERMES_SRC_API_HERMES_H_
#define HERMES_SRC_API_HERMES_H_

#include "config.h"
#include "constants.h"
#include "hermes_types.h"
#include "rpc.h"

namespace hermes::api {

class Hermes {
 public:
  HermesType mode_;
  ServerConfig server_config_;
  ClientConfig client_config_;
  COMM_TYPE comm_;
  RPC_TYPE rpc_;
  lipc::Allocator *main_alloc_;

 public:
  Hermes() = default;
  Hermes(HermesType mode,
         std::string server_config_path = "",
         std::string client_config_path = "");

  void Init(HermesType mode,
            std::string server_config_path = "",
            std::string client_config_path = "");

  void Finalize();

  void RunDaemon();

 private:
  void InitDaemon(std::string server_config_path);

  void InitColocated(std::string server_config_path,
                     std::string client_config_path);

  void InitClient(std::string client_config_path);

  void LoadServerConfig(std::string config_path);

  void LoadClientConfig(std::string config_path);

  void InitSharedMemory();

  void LoadSharedMemory();

  void FinalizeDaemon();

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
