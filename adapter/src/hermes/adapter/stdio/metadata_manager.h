#ifndef HERMES_ADAPTER_METADATA_MANAGER_H
#define HERMES_ADAPTER_METADATA_MANAGER_H

/**
 * Standard headers
 */
#include <ftw.h>

#include <cstdio>
#include <unordered_map>

/**
 * Internal headers
 */
#include <hermes/adapter/stdio/common/constants.h>
#include <hermes/adapter/stdio/common/datastructures.h>

namespace hermes::adapter::stdio {
/**
 * Metadata manager for STDIO adapter
 */
class MetadataManager {
 private:
  /**
   * Private members
   */
  /**
   * Maintain a local metadata FileID structure mapped to Adapter Stats.
   */
  std::unordered_map<FileID, AdapterStat> metadata;
  /**
   * hermes attribute to initialize Hermes
   */
  std::shared_ptr<hapi::Hermes> hermes;
  /**
   * references of how many times hermes was tried to initialize.
   */
  std::atomic<size_t> ref;

 public:
  /**
   * Constructor
   */
  MetadataManager() : metadata(), ref(0) {}
  /**
   * Get the instance of hermes.
   */
  std::shared_ptr<hapi::Hermes>& GetHermes() { return hermes; }

  /**
   * Initialize hermes. Get the kHermesConf from environment else get_env
   * returns NULL which is handled internally by hermes. Initialize hermes in
   * daemon mode. Keep a reference of how many times Initialize is called.
   * Within the adapter, Initialize is called from fopen.
   */
  void InitializeHermes(bool is_mpi = false) {
    if (ref == 0) {
      char* hermes_config = getenv(kHermesConf);
      hermes = hapi::InitHermes(hermes_config, !is_mpi);
    }
    ref++;
  }
  /**
   * Finalize hermes and close rpc if reference is equal to one. Else just
   * decrement the ref counter.
   */
  void FinalizeHermes() {
    if (ref == 1) {
      hermes->Finalize(true);
    }
    ref--;
  }
  /**
   * Convert file handler to FileID using the stat.
   */
  FileID Convert(FILE* fh);

  /**
   * Create a metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination
   * filesystem.
   * @param stat, AdapterStat, STDIO Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Create(FILE* fh, const AdapterStat& stat);

  /**
   * Update existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @param stat, AdapterStat, STDIO Adapter version of Stat data structure.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful or entry doesn't exist.
   */
  bool Update(FILE* fh, const AdapterStat& stat);

  /**
   * Delete existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @return    true, if operation was successful.
   *            false, if operation was unsuccessful.
   */
  bool Delete(FILE* fh);

  /**
   * Find existing metadata entry for STDIO adapter for a given file handler.
   * @param fh, FILE*, original file handler of the file on the destination.
   * @return    The metadata entry if exist.
   *            The bool in pair indicated whether metadata entry exists.
   */
  std::pair<AdapterStat, bool> Find(FILE* fh);
};
}  // namespace hermes::adapter::stdio

#endif  // HERMES_ADAPTER_METADATA_MANAGER_H
