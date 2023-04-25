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

#include "test_init.h"
#include "hermes_shm/data_structures/ipc/string.h"
#include "hermes_shm/data_structures/serialization/thallium.h"
#include "hermes_shm/data_structures/containers/charbuf.h"
#include <memory>

std::unique_ptr<tl::engine> client_;
std::unique_ptr<tl::engine> server_;

template<typename T>
bool VerifyVector(hipc::vector<T> &vec) {
  for (int i = 0; i < 20; ++i) {
    if constexpr(std::is_same_v<T, int>) {
      if (vec[i] != i) {
        return false;
      }
    } else {
      if (vec[i] != std::to_string(i)) {
        return false;
      }
    }
  }
  return true;
}

int main() {
  // Pretest
  ServerPretest<hipc::StackAllocator>();

  // Create thallium server
  server_ = std::make_unique<tl::engine>(
    tcnst::kServerName,
    THALLIUM_SERVER_MODE,
    true, 1);
  std::cout << "Server running at address " << server_->self() << std::endl;

  // Test transfer of 0-length string
  auto string_test0 = [](const request &req,
                         hipc::uptr<hipc::string> &text) {
    bool ret = (*text == "");
    req.respond(ret);
  };
  server_->define(tcnst::kStringTest0, string_test0);

  // Test transfer of long string
  auto string_test1 = [](const request &req,
                         hipc::uptr<hipc::string> &text) {
    bool ret = (*text == tcnst::kTestString);
    req.respond(ret);
  };
  server_->define(tcnst::kStringTestLarge, string_test1);

  // Test transfer of 0-length charbuf
  auto charbuf_test0 = [](const request &req,
                          hshm::charbuf &text) {
    bool ret = (text == "");
    req.respond(ret);
  };
  server_->define(tcnst::kCharbufTest0, charbuf_test0);

  // Test transfer of long charbuf
  auto charbuf_test1 = [](const request &req,
                          hshm::charbuf &text) {
    bool ret = (text == tcnst::kTestString);
    req.respond(ret);
  };
  server_->define(tcnst::kCharbufTestLarge, charbuf_test1);

  // Test transfer of empty vector
  auto vec_of_int0_test = [](const request &req,
                             hipc::uptr<hipc::vector<int>> &vec) {
    bool ret = vec->size() == 0;
    req.respond(ret);
  };
  server_->define(tcnst::kVecOfInt0Test, vec_of_int0_test);

  // Test transfer of large vector
  auto vec_of_int_large_test = [](const request &req,
                                  hipc::uptr<hipc::vector<int>> &vec) {
    bool ret = VerifyVector(*vec);
    req.respond(ret);
  };
  server_->define(tcnst::kVecOfIntLargeTest, vec_of_int_large_test);

  // Test transfer of empty string vector
  auto vec_of_string0_test = [](const request &req,
                                hipc::uptr<hipc::vector<hipc::string>> &vec) {
    bool ret = vec->size() == 0;
    req.respond(ret);
  };
  server_->define(tcnst::kVecOfString0Test, vec_of_string0_test);

  // Test transfer of large string vector
  auto vec_of_string_large_test = [](
    const request &req, hipc::uptr<hipc::vector<hipc::string>> &vec) {
    bool ret = VerifyVector(*vec);
    req.respond(ret);
  };
  server_->define(tcnst::kVecOfStringLargeTest, vec_of_string_large_test);

  // Test transfer of bitfield
  auto bitfield_test = [](const request &req, hshm::bitfield32_t &field) {
    bool ret = field.Any(0x8);
    req.respond(ret);
  };
  server_->define(tcnst::kBitfieldTest, bitfield_test);

  // Start daemon
  server_->enable_remote_shutdown();
  server_->wait_for_finalize();
}
