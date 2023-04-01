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
#include <filesystem>

namespace stdfs = std::filesystem;

namespace hermes {

void TraitManager::Init() {
  ServerConfig *config = &HERMES->server_config_;

  // Load the LD_LIBRARY_PATH variable
  auto ld_lib_path_env = getenv("LD_LIBRARY_PATH");
  if (!ld_lib_path_env) {
    HELOG(kError, "LD_LIBRARY_PATH is not defined. "
                  "Cannot search for traits.");
    return;
  }
  std::string ld_lib_path(ld_lib_path_env);

  // Load the HERMES_TRAIT_PATH variable
  std::string hermes_trait_path;
  auto hermes_trait_path_env = getenv("HERMES_TRAIT_PATH");
  if (hermes_trait_path_env) {
    hermes_trait_path = hermes_trait_path_env;
  }

  // Combine LD_LIBRARY_PATH and HERMES_TRAIT_PATH
  std::string paths = hermes_trait_path + ":" + ld_lib_path;

  // Find each trait in LD_LIBRARY_PATH
  for (std::string &trait_rel_path : config->trait_paths_) {
    if(!FindLoadTrait(paths, trait_rel_path)) {
      HELOG(kWarning, "Failed to load the trait: {}", trait_rel_path);
    }
  }
}

bool TraitManager::FindLoadTrait(const std::string &paths,
                                 const std::string &trait_rel_path) {
  std::stringstream ss(paths);
  std::string trait_dir;
  while (std::getline(ss, trait_dir, ':')) {
    std::string trait_path = hshm::Formatter::format("{}/{}",
                                                     trait_dir,
                                                     trait_rel_path);
    if (!stdfs::exists(trait_path)) {
      continue;
    }
    TraitLibInfo info;
    info.lib_ = dlopen(trait_path.c_str(), RTLD_GLOBAL | RTLD_NOW);
    if (!info.lib_) {
      HELOG(kError, "Could not open the trait library: {}", trait_path);
      return false;
    }
    info.create_trait_ = (create_trait_t)dlsym(info.lib_, "create_trait");
    if (!info.create_trait_) {
      HELOG(kError, "The trait {} does not have create_trait symbol",
            trait_path);
      return false;
    }
    info.get_trait_id_ = (get_trait_id_t)dlsym(info.lib_, "get_trait_id");
    if (!info.get_trait_id_) {
      HELOG(kError, "The trait {} does not have get_trait_id symbol",
            trait_path);
      return false;
    }
    std::string trait_name = info.get_trait_id_();
    HILOG(kDebug, "Finished loading the trait: {}", trait_name)
    traits_.emplace(trait_name, std::move(info));
    return true;
  }
  return false;
}

}  // namespace hermes
