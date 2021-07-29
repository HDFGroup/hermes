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

#include "glog/logging.h"
#include "hermes.h"
#include "traits.h"

namespace hermes {

namespace api {

class VBucket {
 private:
  std::string name_;
  VBucketID id_;
  std::list<Trait *> attached_traits_;
  Blob local_blob;
  /** internal Hermes object owned by vbucket */
  std::shared_ptr<Hermes> hermes_;
  /** The Context for this VBucket. */
  Context ctx_;

 public:
  VBucket(std::string initial_name, std::shared_ptr<Hermes> const &h,
          Context ctx = Context())
      : name_(initial_name),
        id_({{0, 0}}),
        attached_traits_(),
        local_blob(),
        hermes_(h),
        ctx_(ctx) {
    if (IsVBucketNameTooLong(name_)) {
      id_.as_int = 0;
      throw std::length_error("VBucket name exceeds maximum size of " +
                              std::to_string(kMaxVBucketNameSize));
    } else {
      id_ = GetOrCreateVBucketId(&hermes_->context_, &hermes_->rpc_, name_);
      if (!IsValid()) {
        throw std::runtime_error("Could not open or create VBucket");
      }
    }
  }

  ~VBucket() {
    if (IsValid()) {
      Release();
    }
  }

  bool IsValid() const;

  /** get the name of vbucket */
  std::string GetName() const { return this->name_; }

  /**
   *
   */
  void WaitForBackgroundFlush();

  /**
   * Link a Blob to this VBucket.
   *
   * Adds Blob @p blob_name in Bucket @p bucket_name to this VBucket's list of
   * Blobs. Additional calls the Trait::OnLinkFn function on the Blob for each
   * attached Trait.
   *
   * @param blob_name The name of the Blob to link.
   * @param bucket_name The name of the Bucket containing the Blob to link.
   * @param ctx Currently unused.
   *
   * @return A Status.
   */
  Status Link(std::string blob_name, std::string bucket_name, Context &ctx);
  Status Link(std::string blob_name, std::string bucket_name);

  /**
   * Unlink a Blob from this VBucket.
   *
   * @param blob_name The name of the Blob to unlink.
   * @param bucket_name The name of the Bucket containing the Blob to unlink.
   *
   * @return A Status.
   */
  Status Unlink(std::string blob_name, std::string bucket_name, Context &ctx);
  Status Unlink(std::string blob_name, std::string bucket_name);

  /** check if blob is in this vbucket */
  bool ContainsBlob(std::string blob_name, std::string bucket_name);

  /** get a blob linked to this vbucket */
  Blob &GetBlob(std::string blob_name, std::string bucket_name);

  /** retrieves the subset of blob links satisfying pred */
  /** could return iterator */
  std::vector<std::string> GetLinks(Context &ctx);

  /**
   * Attach a trait to this VBucket.
   *
   * Calls the Trait::onAttachFn function of @p trait on each Blob that's linked
   * to this VBucket.
   *
   * @param trait The Trait to attach.
   * @param ctx Currently unused.
   *
   * @return A Status.
   */
  Status Attach(Trait *trait, Context &ctx);
  Status Attach(Trait *trait);

  /** detach a trait to this vbucket */
  Status Detach(Trait *trait, Context &ctx);
  Status Detach(Trait *trait);

  /** retrieves the subset of attached traits satisfying pred */
  template <class Predicate>
  std::vector<TraitID> GetTraits(Predicate pred, Context &ctx);

  /**
   * Get's an attached Trait that matches @p type.
   *
   * @param type The type of Trait to retrieve.
   *
   * @return The first attached trait that matches @p type.
   */
  Trait *GetTrait(TraitType type);

  /**
   * Release this vBucket.
   *
   * This function does not result in any Trait callbacks being invoked or any
   * Blob links to be deleted. It simply decrements the reference count on this
   * VBucket. A VBucket can only be destroyed (VBucket::Destroy) when it's
   * reference count is 1. I.e., each rank that is not destroying the VBucket
   * must release it.
   *
   * @param ctx Currently unused.
   *
   * @return A Status.
   */
  Status Release(Context &ctx);
  Status Release();

  /**
   * Destroy this VBucket.
   *
   * Releases all resources associated with this VBucket. If it is opened again,
   * it will be created from scratch. Unlinks all linked Blobs (which will
   * invoke each attached Trait's Trait::onUnlinkFn function), and detaches all
   * attached Traits, invoking Trait::onDetachFn. If the VBucket::persist member
   * is @c true, the linked Blobs will be flushed according to the attached
   * FileMappingTrait, if any.
   *
   * @param ctx Currently unused.
   *
   * @return A Status.
   */
  Status Destroy(Context &ctx);
  Status Destroy();
};  // class VBucket

}  // namespace api
}  // namespace hermes

#endif  //  VBUCKET_H_
