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

#ifndef HERMES_TEST_UTILS_H_
#define HERMES_TEST_UTILS_H_

#include <stdio.h>
#include <stdlib.h>

#include <chrono>
#include <iostream>
#include <vector>
#include <map>

#include "hermes_types.h"
#include "bucket.h"

namespace hermes {
namespace testing {

// TODO(chogan): Keep time in generic units and only convert the duration at the
// end
class Timer {
 public:
  Timer():elapsed_time(0) {}
  void resumeTime() {
    t1 = std::chrono::high_resolution_clock::now();
  }
  double pauseTime() {
    auto t2 = std::chrono::high_resolution_clock::now();
    elapsed_time += std::chrono::duration<double>(t2 - t1).count();
    return elapsed_time;
  }
  double getElapsedTime() {
    return elapsed_time;
  }
 private:
  std::chrono::high_resolution_clock::time_point t1;
  double elapsed_time;
};

void Assert(bool expr, const char *file, int lineno, const char *message) {
  if (!expr) {
    fprintf(stderr, "Assertion failed at %s: line %d: %s\n", file, lineno,
            message);
    exit(-1);
  }
}

#define Assert(expr) hermes::testing::Assert((expr), __FILE__, __LINE__, #expr)

TargetID DefaultRamTargetId() {
  TargetID result = {};
  result.bits.node_id = 1;
  result.bits.device_id = 0;
  result.bits.index = 0;

  return result;
}

TargetID DefaultFileTargetId() {
  TargetID result = {};
  result.bits.node_id = 1;
  result.bits.device_id = 1;
  result.bits.index = 1;

  return result;
}

void GetAndVerifyBlob(api::Bucket &bucket, const std::string &blob_name,
                      const api::Blob &expected) {
  api::Context ctx;
  api::Blob retrieved_blob;
  size_t expected_size = expected.size();
  size_t retrieved_size = bucket.Get(blob_name, retrieved_blob, ctx);
  Assert(expected_size == retrieved_size);
  retrieved_blob.resize(retrieved_size);
  retrieved_size = bucket.Get(blob_name, retrieved_blob, ctx);
  Assert(expected_size == retrieved_size);
  Assert(retrieved_blob == expected);
}

}  // namespace testing
}  // namespace hermes

#endif  // HERMES_TEST_UTILS_H_
