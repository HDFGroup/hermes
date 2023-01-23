#include "singleton.h"

#include "filesystem.h"
template<> std::unique_ptr<hermes::adapter::fs::MetadataManager> hermes::Singleton<hermes::adapter::posix::PosixApi>::obj_ = nullptr;
