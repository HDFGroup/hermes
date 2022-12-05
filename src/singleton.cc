#include "singleton.h"

#include <hermes.h>
template<> std::unique_ptr<hermes::api::Hermes> hermes::Singleton<hermes::api::Hermes>::obj_ = nullptr;
