#include "singleton.h"

#include "posix_api.h"
template<> hermes::adapter::fs::PosixApi hermes::GlobalSingleton<hermes::adapter::fs::PosixApi>::obj_ = hermes::adapter::fs::PosixApi();
