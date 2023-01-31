#include "singleton.h"

#include "posix_api.h"
template<> std::unique_ptr<hermes::adapter::posix::PosixApi> hermes::Singleton<hermes::adapter::posix::PosixApi>::obj_ = nullptr;
