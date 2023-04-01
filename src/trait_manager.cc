/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
* Distributed under BSD 3-Clause license.                                   *
* Copyright by The HDF Group.                                               *
* Copyright by the Illinois Institute of Technology.                        *
* All rights reserved.                                                      *
*                                                                           *
* This file is part of Hermes. The full Hermes copyright notice, including  *
* terms governing use, modification, and redistribution, is contained in    *
* the COPYING file, which can be found at the top directory. If you do not  *
* have access to the file, you may request a copy from help@hdfgroup.org.   *
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

#include "hermes_shm/util/singleton.h"
#include "hermes.h"
#include "trait_manager.h"

namespace hermes {

void TraitManager::Init() {
  ServerConfig *config = &HERMES->server_config_;
  for (std::string &trait_path : config->trait_paths_) {
    TraitLibInfo info;
    info.lib_ = dlopen(trait_path.c_str(), RTLD_GLOBAL | RTLD_NOW);
    if (!info.lib_) {
      HELOG(kError, "Could not open the trait library: {}", trait_path);
      continue;
    }
    info.create_trait_ = (create_trait_t)dlsym(info.lib_, "create_trait");
    if (!info.create_trait_) {
      HELOG(kError, "The trait {} does not have create_trait symbol");
      continue;
    }
    info.get_trait_id_ = (get_trait_id_t)dlsym(info.lib_, "get_trait_id");
    if (!info.get_trait_id_) {
      HELOG(kError, "The trait {} does not have get_trait_id symbol");
      continue;
    }
    std::string trait_name = info.get_trait_id_();
    HILOG(kDebug, "Finished loading the trait: {}", trait_name)
    traits_.emplace(trait_name, std::move(info));
  }
}

}  // namespace hermes
