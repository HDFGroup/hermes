#include "singleton.h"

#include "filesystem_mdm.h"
template<> hermes::adapter::fs::MetadataManager
hermes::GlobalSingleton<hermes::adapter::fs::MetadataManager>::obj_ = hermes::adapter::fs::MetadataManager();
