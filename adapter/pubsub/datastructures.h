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

#ifndef HERMES_STDIO_ADAPTER_DATASTRUCTURES_H
#define HERMES_STDIO_ADAPTER_DATASTRUCTURES_H

/**
 * Standard header
 */
#include <ftw.h>
#include <string>

/**
 * Internal header
 */
#include <bucket.h>
#include <buffer_pool.h>
#include <hermes_types.h>

/**
 * Namespace simplification.
 */
namespace hapi = hermes::api;

namespace hermes::adapter::pubsub {

/**
 * Struct that defines the metadata required for the pubsub adapter.
 */
struct ClientMetadata {
  /**
   * attributes
   */
  std::shared_ptr<hapi::Bucket> st_bkid; /* bucket associated with the topic */
  u64 last_subscribed_blob; /* Current blob being used */
  timespec st_atim;     /* time of last access */
  /**
   * Constructor
   */
  ClientMetadata()
      : st_bkid(),
        last_subscribed_blob(0),
        st_atim() {} /* default constructor */
  ClientMetadata(const struct ClientMetadata &st)
      : st_bkid(st.st_bkid),
        last_subscribed_blob(st.last_subscribed_blob),
        st_atim(st.st_atim) {} /* parameterized constructor */
};

} // hermes::adapter::pubsub

#endif  // HERMES_STDIO_ADAPTER_DATASTRUCTURES_H
