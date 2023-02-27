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

#ifndef HERMES_SRC_API_TRAITS_H_
#define HERMES_SRC_API_TRAITS_H_

namespace hermes::api {

class VBucket;

class Trait {
 public:
  /** Called when AttachTrait is called on VBucket */
  virtual void Attach(VBucket *vbkt) = 0;

  /** Called when DetachTrait is called on VBucket */
  virtual void Detach(VBucket *vbkt) = 0;

  /** Called when a Link is called on VBucket*/
  virtual void OnLink(VBucket *vbkt) = 0;

  /** Called when Unlink is called on VBucket*/
  virtual void OnUnlink(VBucket *vbkt) = 0;

  /** Called when GetLinks is called on VBucket */
  virtual void OnGet(VBucket *vbkt) = 0;
};

}  // namespace hermes::api

#endif  // HERMES_SRC_API_TRAITS_H_
