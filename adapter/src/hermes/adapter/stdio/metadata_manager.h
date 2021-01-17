//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_METADATA_MANAGER_H
#define HERMES_METADATA_MANAGER_H

#include <ftw.h>
#include <hermes/adapter/stdio/common/constants.h>
#include <hermes/adapter/stdio/common/datastructures.h>

#include <cstdio>
#include <unordered_map>

namespace hermes::adapter::stdio {
class MetadataManager {
 private:
  std::unordered_map<FileID, AdapterStat> metadata;
  std::shared_ptr<hapi::Hermes> hermes;
  size_t ref;

 public:
  MetadataManager() : metadata(), ref(0) {}

  std::shared_ptr<hapi::Hermes>& GetHermes() { return hermes; }

  void InitializeHermes() {
    if (ref == 0) {
      char* hermes_config = getenv(HERMES_CONF);
      hermes = hapi::InitHermes(hermes_config, true);
    }
    ref++;
  }

  void FinalizeHermes() {
    if (ref == 1) {
      hermes->Finalize(true);
    }
    ref--;
  }

  FileID convert(FILE* fh);

  bool IsTracked(FILE* fh);

  bool Create(FILE* fh, const AdapterStat& stat);

  bool Update(FILE* fh, const AdapterStat& stat);

  bool Delete(FILE* fh);

  std::pair<AdapterStat, bool> Find(FILE* fh);
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_METADATA_MANAGER_H
