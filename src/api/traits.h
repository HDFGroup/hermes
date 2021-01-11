#ifndef HERMES_TRAITS_H
#define HERMES_TRAITS_H

#include <unordered_map>

#include "hermes_types.h"

namespace hermes {
namespace api {
struct BlobInfo {
  std::string bucket_name;
  std::string blob_name;
  Blob blob;
};
typedef BlobInfo TraitInput;
// typedef void* TraitCallback;
// typedef void(*TraitCallback)(hermes::api::Blob &blob, void *trait);
typedef std::function<void(TraitInput &, void *)> TraitCallback;
struct Trait {
  TraitID id;
  TraitIdArray conflict_traits;
  TraitType type;
  TraitCallback onAttachFn;
  TraitCallback onDetachFn;
  TraitCallback onLinkFn;
  TraitCallback onUnlinkFn;
  Trait(TraitID id, TraitIdArray conflict_traits, TraitType type);
  void onAttach(TraitInput &blob, void *trait) {}
  void onDetach(TraitInput &blob, void *trait) {}
  void onLink(TraitInput &blob, void *trait) {}
  void onUnlink(TraitInput &blob, void *trait) {}
};

#define FILE_TRAIT 10

struct FileBackedTrait : public Trait {
 private:
  std::string filename;
  std::unordered_map<std::string, u64> offset_map;
  bool flush;
  bool load;
  TraitCallback flush_cb;
  TraitCallback load_cb;

 public:
  FileBackedTrait(std::string &filename,
                  std::unordered_map<std::string, u64> &offset_map, bool flush,
                  TraitCallback flush_cb, bool load, TraitCallback load_cb);
  void onAttach(TraitInput &blob, void *trait);
  void onDetach(TraitInput &blob, void *trait);
  void onLink(TraitInput &blob, void *trait);
  void onUnlink(TraitInput &blob, void *trait);
};
}  // namespace api
}  // namespace hermes

#endif  // HERMES_TRAITS_H
