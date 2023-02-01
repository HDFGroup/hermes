#include "singleton.h"

#include "fs_metadata_manager.h"
template<> std::unique_ptr<hermes::adapter::fs::MetadataManager> hermes::Singleton<hermes::adapter::fs::MetadataManager>::obj_ = nullptr;
