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

#define HERMES_PERSIST_TRAIT 11
#define HERMES_WRITE_ONLY_TRAIT 12

/** A blob's hosting bucket and blob names */
struct BlobInfo {
  /** The blob-hosting bucket name */
  std::string bucket_name;
  /** The blob's name (in the bucket) */
  std::string blob_name;
};

typedef BlobInfo TraitInput;
struct Trait;
using HermesPtr = std::shared_ptr<Hermes>;

// TODO(chogan): I don't think we need to pass a Trait* to these callbacks
// anymore. That is a relic of an old implementation.

/** Callback for blob->vbucket link events */
typedef std::function<void(HermesPtr, TraitInput &, Trait *)> OnLinkCallback;
/** Callback for trait->vbucket attach events */
typedef std::function<void(HermesPtr, VBucketID, Trait *)> OnAttachCallback;

/** \brief Base class for Trait%s, which can attach functionality to VBucket%s.
 *
 * To add functionality to a VBucket, inherit from this class and implement the
 * various callbacks.
 */
struct Trait {
  /** The trait's ID */
  TraitID id;
  /** IDs of Trait%s whose functionality conflict with this Trait. */
  std::vector<TraitID> conflict_traits;
  /** The trait's type. */
  TraitType type;
  /** Callback for trait->vbucket attach events. */
  OnAttachCallback onAttachFn;
  /** Callback for trait->vbucket detach events. */
  OnAttachCallback onDetachFn;
  /** Callback for blob->vbucket link events. */
  OnLinkCallback onLinkFn;
  /** Callback for blob-<vbucket unlink events. */
  OnLinkCallback onUnlinkFn;
  /** Callback for VBucket::Get events. */
  OnLinkCallback onGetFn;

  /** \brief Default constructor.
   *
   */
  Trait() {}

  /** \brief Construct a Trait.
   *
   * \param id A unique identifier.
   * \param conflict_traits The IDs of the Traits that conflict with this Trait.
   * \param type The type of Trait.
   */
  Trait(TraitID id, const std::vector<TraitID> &conflict_traits,
        TraitType type);
};

/** \brief Engable persisting a <tt>VBucket</tt>'s linked Blob%s to permanent
 * storage.
 *
 */
struct PersistTrait : public Trait {
  /** The name of the file to flush the Blob%s to. */
  std::string filename;
  /** Maps Blob names to offsets within a file. */
  std::unordered_map<std::string, u64> offset_map;
  /** \bool{flushing data should block until finished} */
  bool synchronous;

  /** */
  explicit PersistTrait(bool synchronous);
  /** */
  explicit PersistTrait(const std::string &filename,
                        const std::unordered_map<std::string, u64> &offset_map,
                        bool synchronous = false);

  /**
   *
   */
  void onAttach(HermesPtr hermes, VBucketID id, Trait *trait);

  /** \brief Currently a no-op. */
  void onDetach(HermesPtr hermes, VBucketID id, Trait *trait);

  /**
   *
   */
  void onLink(HermesPtr hermes, TraitInput &input, Trait *trait);

  /** \brief Currently a no-op. */
  void onUnlink(HermesPtr hermes, TraitInput &input, Trait *trait);
};

/** \brief Marks the Blob%s in a VBucket as write-only.
 *
 * If we know that certain Blob%s are write-only, we can asynchronously and
 * eagerly flush buffered data to the final destination.
 *
 */
struct WriteOnlyTrait : public Trait {
  /** */
  WriteOnlyTrait();

  /** \brief Currently a no-op. */
  void onAttach(HermesPtr hermes, VBucketID id, Trait *trait);

  /** \brief Currently a no-op. */
  void onDetach(HermesPtr hermes, VBucketID id, Trait *trait);

  /**
   *
   */
  void onLink(HermesPtr hermes, TraitInput &input, Trait *trait);

  /** \brief Currently a no-op. */
  void onUnlink(HermesPtr hermes, TraitInput &input, Trait *trait);
};

}  // namespace api
}  // namespace hermes

#endif  // HERMES_TRAITS_H
