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
 * Dependent library header
 */

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
struct TopicMetadata {
  /**
   * attributes
   */
  std::shared_ptr<hapi::Bucket> st_bkid; /* bucket associated with the file */
  std::set<std::string, bool (*)(const std::string &, const std::string &)>
      st_blobs;         /* Blobs access in the bucket */
  i32 ref_count;        /* # of time process opens a file */
  u32 current_blob;     /* Current blob being used */
  u32 final_blob;       /* Final blob of the topic */
  timespec st_atim;     /* time of last access */
  /**
   * Constructor
   */
  TopicMetadata()
      : st_bkid(),
        st_blobs(CompareBlobs),
        ref_count(),
        current_blob(),
        final_blob(),
        st_atim() {} /* default constructor */
  explicit TopicMetadata(const struct TopicMetadata &st)
      : st_bkid(),
        st_blobs(CompareBlobs),
        ref_count(1),
        current_blob(st.current_blob),
        final_blob(st.final_blob),
        st_atim(st.st_atim) {} /* parameterized constructor */

  /**
   * Comparator for comparing two blobs.
   */
  static bool CompareBlobs(const std::string &a, const std::string &b) {
    return std::stol(a) < std::stol(b);
  }
};

}  // namespace hermes::adapter::stdio

#endif  // HERMES_STDIO_ADAPTER_DATASTRUCTURES_H
