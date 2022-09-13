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

#ifndef VBUCKET_H_
#define VBUCKET_H_

#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include "hermes.h"
#include "traits.h"

namespace hermes {

namespace api {

/**
 * Virtual buckets (VBucket%s) capture relationships between Blob%s
 * across Bucket boundaries.
 */
class VBucket {
 private:
  /** The user-facing name of this VBucket. */
  std::string name_;
  /** The internal ID of this VBucket. */
  VBucketID id_;
  /** Traits attached to this vbucket. */
  std::list<Trait *> attached_traits_;
  /** The Hermes instance this VBucket is stored in. */
  std::shared_ptr<Hermes> hermes_;
  /** The Context for this VBucket. Overrides the global default Context. */
  Context ctx_;

 public:
  /** \brief
   *
   */
  VBucket(std::string initial_name, std::shared_ptr<Hermes> const &h,
          Context ctx = Context());

  /** \brief
   *
   */
  ~VBucket();

  /** \brief Return bool{this VBucket is valid}
   *
   * A VBucket is valid if it has a non-NULL ID, meaning it has been registered
   * in the Hermes system.
   *
   * \return \bool{this VBucket is valid}
   */
  bool IsValid() const;

  /** \brief Return the name of this VBucket.
   *
   * \return The name of this VBucket.
   */
  std::string GetName() const { return this->name_; }

  /**
   * Blocks until all outstanding asynchronous flushing tasks associated with
   * this VBucket are complete.
   */
  void WaitForBackgroundFlush();

  /**
   * Link a Blob to this VBucket.
   *
   * Adds Blob \p blob_name in Bucket \p bucket_name to this VBucket's list of
   * Blobs. Additional calls the Trait::OnLinkFn function on the Blob for each
   * attached Trait.
   *
   * \param blob_name The name of the Blob to link.
   * \param bucket_name The name of the Bucket containing the Blob to link.
   *
   * \return \status
   */
  Status Link(std::string blob_name, std::string bucket_name);

  /** \overload
   *
   * \param ctx Currently unused.
   */
  Status Link(std::string blob_name, std::string bucket_name, Context &ctx);

  /**
   * Unlink a Blob from this VBucket.
   *
   * \param blob_name The name of the Blob to unlink.
   * \param bucket_name The name of the Bucket containing the Blob to unlink.
   *
   * \return \status
   */
  Status Unlink(std::string blob_name, std::string bucket_name, Context &ctx);
  /** \todo Unlink */
  Status Unlink(std::string blob_name, std::string bucket_name);

  /** check if blob is in this vbucket */
  bool ContainsBlob(std::string blob_name, std::string bucket_name);

  /** Get a Blob, calling any OnGet callbacks of attached Traits.
   *
   * Exactly like Bucket::Get, except this function invokes the OnGet callback
   * of any attached Traits.
   */
  size_t Get(const std::string &name, Bucket &bkt, Blob &user_blob,
             const Context &ctx);
  size_t Get(const std::string &name, Bucket &bkt, Blob &user_blob);
  size_t Get(const std::string &name, Bucket &bkt, void *user_blob,
             size_t blob_size, const Context &ctx);

  /** retrieves the subset of blob links satisfying pred */
  /** could return iterator */
  std::vector<std::string> GetLinks(Context &ctx);

  /** \brief Attach a Trait to this VBucket.
   *
   * Calls the Trait::onAttachFn function of \p trait on each Blob that's linked
   * to this VBucket.
   *
   * \param trait The Trait to attach.
   *
   * \return \status
   */
  Status Attach(Trait *trait);
  /** \overload
   *
   * \param ctx Currently unused.
   */
  Status Attach(Trait *trait, Context &ctx);

  /** \brief Detach a trait from this VBucket.
   *
   * \param trait The Trait to detach.
   *
   * \return \status
   */
  Status Detach(Trait *trait);

  /** \overload
   *
   * \param ctx Currently unused.
   */
  Status Detach(Trait *trait, Context &ctx);

  /** \brief Retrieves the subset of attached traits satisfying the Predicate \p pred.
   *
   * \todo \p pred is curently ignored and this function returns all attached
   * traits.
   *
   * \param pred \todo
   * \param ctx Currently unused;
   */
  template <class Predicate>
  std::vector<TraitID> GetTraits(Predicate pred, Context &ctx);

  /** \brief Get's an attached Trait that matches \p type.
   *
   * \param type The type of Trait to retrieve.
   *
   * \return The first attached trait that matches \p type.
   */
  Trait *GetTrait(TraitType type);

  /** \brief Release this vBucket.
   *
   * This function does not result in any Trait callbacks being invoked or any
   * Blob links to be deleted. It simply decrements the reference count on this
   * VBucket. A VBucket can only be destroyed (VBucket::Destroy) when it's
   * reference count is 1. I.e., each rank that is not destroying the VBucket
   * must release it.
   *
   * \return A Status.
   */
  Status Release();

  /** \overload
   *
   * \param ctx Currently unused.
   */
  Status Release(Context &ctx);

  /** \brief Destroy this VBucket.
   *
   * Releases all resources associated with this VBucket. If it is opened again,
   * it will be created from scratch. Unlinks all linked Blobs (which will
   * invoke each attached Trait's Trait::onUnlinkFn function), and detaches all
   * attached Traits, invoking Trait::onDetachFn.
   *
   * \return \status
   */
  Status Destroy();

  /** \overload
   *
   * \param ctx Currently unused.
   */
  Status Destroy(Context &ctx);
};  // class VBucket

}  // namespace api
}  // namespace hermes

#endif  //  VBUCKET_H_
