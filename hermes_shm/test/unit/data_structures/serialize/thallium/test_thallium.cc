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

#include "basic_test.h"
#include "test_init.h"
#include "hermes_shm/data_structures/ipc/string.h"

TEST_CASE("SerializeString") {
  tl::endpoint server = client_->lookup(tcnst::kServerName);
  tl::remote_procedure string0_proc = client_->define(tcnst::kStringTest0);
  tl::remote_procedure string_large_proc = client_->define(
    tcnst::kStringTestLarge);

  auto empty_str = hipc::make_uptr<hipc::string>("");
  auto large_str = hipc::make_uptr<hipc::string>(tcnst::kTestString);

  REQUIRE(string0_proc.on(server)(empty_str));
  REQUIRE(string_large_proc.on(server)(large_str));
}

TEST_CASE("SerializeCharBuf") {
  tl::endpoint server = client_->lookup(tcnst::kServerName);
  tl::remote_procedure string0_proc = client_->define(
    tcnst::kCharbufTest0);
  tl::remote_procedure string_large_proc = client_->define(
    tcnst::kCharbufTestLarge);

  hshm::charbuf empty_str("");
  hshm::charbuf large_str(tcnst::kTestString);

  REQUIRE(string0_proc.on(server)(empty_str));
  REQUIRE(string_large_proc.on(server)(large_str));
}

TEST_CASE("SerializeVectorOfInt") {
  tl::endpoint server = client_->lookup(tcnst::kServerName);
  tl::remote_procedure vec_int0_proc = client_->define(
    tcnst::kVecOfInt0Test);
  tl::remote_procedure vec_int_proc = client_->define(
    tcnst::kVecOfIntLargeTest);

  // Send empty vector
  auto vec_int = hipc::make_uptr<hipc::vector<int>>();
  REQUIRE(vec_int0_proc.on(server)(vec_int));

  // Send initialized vector
  for (int i = 0; i < 20; ++i) {
    vec_int->emplace_back(i);
  }
  REQUIRE(vec_int_proc.on(server)(vec_int));
}

TEST_CASE("SerializeVectorOfString") {
  tl::endpoint server = client_->lookup(tcnst::kServerName);
  tl::remote_procedure vec_string0_proc = client_->define(
    tcnst::kVecOfString0Test);
  tl::remote_procedure vec_string_proc = client_->define(
    tcnst::kVecOfStringLargeTest);

  // Send empty vector
  auto vec_string = hipc::make_uptr<hipc::vector<hipc::string>>();
  REQUIRE(vec_string0_proc.on(server)(vec_string));

  // Send initialized vector
  for (int i = 0; i < 20; ++i) {
    vec_string->emplace_back(std::to_string(i));
  }
  REQUIRE(vec_string_proc.on(server)(vec_string));
}

TEST_CASE("SerializeBitfield") {
  tl::endpoint server = client_->lookup(
    tcnst::kServerName);
  tl::remote_procedure bitfield_proc = client_->define(
    tcnst::kBitfieldTest);

  // Send bitfield
  hshm::bitfield32_t field;
  field.SetBits(0x8);
  REQUIRE(bitfield_proc.on(server)(field));
}

