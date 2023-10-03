//
// Created by lukemartinlogan on 9/30/23.
//

#ifndef HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_
#define HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_

#include "../data_stager.h"

namespace hermes::data_stager {

class AbstractStager {
 public:
  std::string url_;

  AbstractStager() = default;
  ~AbstractStager() = default;

  virtual void RegisterStager(RegisterStagerTask *task, RunContext &rctx) = 0;
  virtual void StageIn(blob_mdm::Client &blob_mdm, StageInTask *task, RunContext &rctx) = 0;
  virtual void StageOut(blob_mdm::Client &blob_mdm, StageOutTask *task, RunContext &rctx) = 0;

  /** Parse url */
  static void GetUrlProtocolAndAction(const std::string &url,
                                      std::string &protocol,
                                      std::string &action,
                                      std::vector<std::string> &tokens) {
    // Determine the protocol + action
    std::string token;
    size_t found = url.find("://");
    if (found != std::string::npos) {
      protocol = url.substr(0, found);
      action = url.substr(found + 3);
    }

    // Divide action into tokens
    std::stringstream ss(action);
    while (std::getline(ss, token, ':')) {
      tokens.push_back(token);
    }
  }

  /** Bucket name from url */
  static hshm::charbuf GetBucketNameFromUrl(const hshm::string &url) {
    // Parse url
    std::string protocol, action;
    std::vector<std::string> tokens;
    GetUrlProtocolAndAction(url, protocol, action, tokens);
    // file://[path]:[page_size]
    if (protocol == "file") {
      std::string path = tokens[0];
      size_t page_size = std::stoul(tokens[1]);
      return hshm::charbuf(path);
    }
  }
};

}  // namespace hermes

#endif  // HERMES_TASKS_DATA_STAGER_SRC_ABSTRACT_STAGER_H_
