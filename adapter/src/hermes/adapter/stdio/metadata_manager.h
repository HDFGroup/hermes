//
// Created by manihariharan on 12/23/20.
//

#ifndef HERMES_METADATA_MANAGER_H
#define HERMES_METADATA_MANAGER_H

#include <ftw.h>
#include <hermes/adapter/stdio/common/datastructures.h>

#include <cstdio>
#include <unordered_map>

namespace hermes::adapter::stdio {
class MetadataManager {
 private:
  std::unordered_map<FileID, AdapterStat> metadata;

  FileID convert(FILE* fh);

 public:
  std::shared_ptr<hapi::Hermes> hermes;

  MetadataManager() : metadata() {}

  bool IsTracked(FILE* fh);

  bool Create(FILE* fh, const AdapterStat& stat);

  bool Update(FILE* fh, const AdapterStat& stat);

  bool Delete(FILE* fh);

  std::pair<AdapterStat, bool> Find(FILE* fh);
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_METADATA_MANAGER_H
