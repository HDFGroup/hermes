//
// Created by lukemartinlogan on 2/4/23.
//

#ifndef HERMES_ADAPTER_ADAPTER_TYPES_H_
#define HERMES_ADAPTER_ADAPTER_TYPES_H_

namespace hermes::adapter {

/** Adapter types */
enum class AdapterType {
  kNone,
  kPosix,
  kStdio,
  kMpiio,
  kPubsub,
  kVfd
};

/** Adapter modes */
enum class AdapterMode {
  kDefault,
  kBypass,
  kScratch,
  kWorkflow
};

/**
 * Per-Object Adapter Settings.
 * An object may be a file, for example.
 * */
struct AdapterObjectConfig {
  AdapterMode mode_;
  size_t page_size_;
};

/** Adapter Mode converter */
class AdapterModeConv {
 public:
  static std::string str(AdapterMode mode) {
    switch(mode) {
      case AdapterMode::kDefault: {
        return "AdapterMode::kDefault";
      }
      case AdapterMode::kBypass: {
        return "AdapterMode::kBypass";
      }
      case AdapterMode::kScratch: {
        return "AdapterMode::kScratch";
      }
      case AdapterMode::kWorkflow: {
        return "AdapterMode::kWorkflow";
      }
    }
  }

  static AdapterMode to_enum(const std::string &mode) {
    if (mode.find("kDefault") != std::string::npos) {
      return AdapterMode::kDefault;
    } else if (mode.find("kBypass") != std::string::npos) {
      return AdapterMode::kBypass;
    } else if (mode.find("kScratch") != std::string::npos) {
      return AdapterMode::kScratch;
    } else if (mode.find("kWorkflow") != std::string::npos) {
      return AdapterMode::kWorkflow;
    }
    return AdapterMode::kDefault;
  }
};


}  // namespace hermes

#endif  // HERMES_ADAPTER_ADAPTER_TYPES_H_
