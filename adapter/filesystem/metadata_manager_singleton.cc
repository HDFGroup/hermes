#include "singleton.h"

#include "filesystem.h"
template<> std::unique_ptr<hermes::adapter::fs::MetadataManager> hermes::Singleton<hermes::adapter::fs::MetadataManager>::obj_ = nullptr;
