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

#ifndef HERMES_TRAITS_H
#define HERMES_TRAITS_H

#include <unordered_map>

#include "hermes_types.h"
#include "hermes.h"

namespace hermes {
namespace api {

struct BlobInfo {
  std::string bucket_name;
  std::string blob_name;
};

typedef BlobInfo TraitInput;
struct Trait;
using HermesPtr = std::shared_ptr<Hermes>;

typedef std::function<void(HermesPtr, TraitInput &, Trait *)> OnLinkCallback;
typedef std::function<void(HermesPtr, VBucketID, Trait *)> OnAttachCallback;

struct Trait {
  TraitID id;
  TraitIdArray conflict_traits;
  TraitType type;
  OnAttachCallback onAttachFn;
  OnAttachCallback onDetachFn;
  OnLinkCallback onLinkFn;
  OnLinkCallback onUnlinkFn;

  Trait() {}
  Trait(TraitID id, TraitIdArray conflict_traits, TraitType type);
};

#define HERMES_FILE_TRAIT 10
#define HERMES_PERSIST_TRAIT 11

struct FileMappingTrait : public Trait {
  OnLinkCallback flush_cb;
  OnLinkCallback load_cb;
  std::string filename;
  std::unordered_map<std::string, u64> offset_map;
  FILE *fh;

  FileMappingTrait() {}
  FileMappingTrait(const std::string &filename,
                   std::unordered_map<std::string, u64> &offset_map, FILE *fh,
                   OnLinkCallback flush_cb, OnLinkCallback load_cb);
  void onAttach(HermesPtr hermes, VBucketID id, Trait *trait);
  void onDetach(HermesPtr hermes, VBucketID id, Trait *trait);
  void onLink(HermesPtr hermes, TraitInput &blob, Trait *trait);
  void onUnlink(HermesPtr hermes, TraitInput &blob, Trait *trait);
};

struct PersistTrait : public Trait {
  FileMappingTrait file_mapping;
  bool synchronous;

  explicit PersistTrait(bool synchronous);
  explicit PersistTrait(FileMappingTrait mapping,
                        bool synchronous = false);

  void onAttach(HermesPtr hermes, VBucketID id, Trait *trait);
  void onDetach(HermesPtr hermes, VBucketID id, Trait *trait);
  void onLink(HermesPtr hermes, TraitInput &blob, Trait *trait);
  void onUnlink(HermesPtr hermes, TraitInput &blob, Trait *trait);
};

}  // namespace api
}  // namespace hermes

#endif  // HERMES_TRAITS_H
