#include "singleton.h"

#include <hermes.h>
template<> hermes::api::Hermes hermes::GlobalSingleton<hermes::api::Hermes>::obj_;
