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

#include "rpc_thallium.h"
#include "metadata_manager.h"
#include "rpc_thallium_serialization.h"
#include "data_structures.h"

namespace hermes {

using thallium::request;

void ThalliumRpc::DefineRpcs() {
  RPC_CLASS_INSTANCE_DEFS_START
  MetadataManager *mdm = this->mdm_;
  RPC_CLASS_INSTANCE_DEFS_END

  RPC_AUTOGEN_START
  RPC_AUTOGEN_END
}

}  // namespace hermes
