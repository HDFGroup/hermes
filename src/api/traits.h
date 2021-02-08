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

namespace hermes {
namespace api {

struct BlobInfo {
  std::string bucket_name;
  std::string blob_name;
};

typedef BlobInfo TraitInput;
struct Trait;
typedef std::function<void(TraitInput &, Trait *)> TraitCallback;

struct Trait {
  TraitID id;
  TraitIdArray conflict_traits;
  TraitType type;
  TraitCallback onAttachFn;
  TraitCallback onDetachFn;
  TraitCallback onLinkFn;
  TraitCallback onUnlinkFn;
  Trait(TraitID id, TraitIdArray conflict_traits, TraitType type);
};

#define HERMES_FILE_TRAIT 10

struct FileMappingTrait : public Trait {
 public:
  TraitCallback flush_cb;
  TraitCallback load_cb;
  std::string filename;
  std::unordered_map<std::string, u64> offset_map;
  FILE *fh;
  FileMappingTrait(std::string &filename,
                   std::unordered_map<std::string, u64> &offset_map, FILE *fh,
                   TraitCallback flush_cb, TraitCallback load_cb);
  void onAttach(TraitInput &blob, Trait *trait);
  void onDetach(TraitInput &blob, Trait *trait);
  void onLink(TraitInput &blob, Trait *trait);
  void onUnlink(TraitInput &blob, Trait *trait);
};
}  // namespace api
}  // namespace hermes

#endif  // HERMES_TRAITS_H
