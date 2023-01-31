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

#include "catch_config.h"
#include "adapter_utils.h"

namespace stdfs = std::experimental::filesystem;

int init(int* argc, char*** argv) {
  (void)argc;
  (void)argv;
  return 0;
}

int finalize() {
  return 0;
}

cl::Parser define_options() {
  return cl::Parser();
}

// NOTE(chogan) GCC's test for weakly_canonical
TEST_CASE("WeaklyCanonical") {
  namespace had = hermes::adapter;

  const std::error_code bad_ec = make_error_code(std::errc::invalid_argument);
  std::error_code ec;

  auto dir = stdfs::path("tmp");
  if (stdfs::exists(dir)) {
    stdfs::remove_all(dir, ec);
  }

  stdfs::create_directory(dir);
  const auto dirc = stdfs::canonical(dir);
  stdfs::path foo = dir/"foo", bar = dir/"bar";
  stdfs::create_directory(foo);
  stdfs::create_directory(bar);
  stdfs::create_directory(bar/"baz");
  stdfs::path p;

  stdfs::create_symlink("../bar", foo/"bar");

  p = had::WeaklyCanonical(dir/"foo//./bar///../biz/.");
  REQUIRE(p == dirc/"biz");
  p = had::WeaklyCanonical(dir/"foo/.//bar/././baz/.");
  REQUIRE(p == dirc/"bar/baz");
  p = had::WeaklyCanonical(stdfs::current_path()/dir/"bar//../foo/bar/baz");
  REQUIRE(p == dirc/"bar/baz");

  ec = bad_ec;
  p = had::WeaklyCanonical(dir/"foo//./bar///../biz/.", ec);
  REQUIRE(!ec);
  REQUIRE(p == dirc/"biz");
  ec = bad_ec;
  p = had::WeaklyCanonical(dir/"foo/.//bar/././baz/.", ec);
  REQUIRE(!ec);
  REQUIRE(p == dirc/"bar/baz");
  ec = bad_ec;
  p = had::WeaklyCanonical(stdfs::current_path()/dir/"bar//../foo/bar/baz", ec);
  REQUIRE(!ec);
  REQUIRE(p == dirc/"bar/baz");

  ec = bad_ec;
  p = had::WeaklyCanonical(dir/"bar/", ec);
  REQUIRE(!ec);
  REQUIRE(p == dirc/"bar");

  // As above, but using "foo/.." instead of "foo",
  // because there is no "foo/bar" symlink

  p = had::WeaklyCanonical(dir/"./bar///../biz/.");
  REQUIRE(p == dirc/"biz");
  p = had::WeaklyCanonical(dir/"foo/.././/bar/././baz/.");
  REQUIRE(p == dirc/"bar/baz");
  p = had::WeaklyCanonical(stdfs::current_path()/dir/"bar//../foo/../bar/baz");
  REQUIRE(p == dirc/"bar/baz");

  ec = bad_ec;
  p = had::WeaklyCanonical(dir/"foo/..//./bar///../biz/.", ec);
  REQUIRE(!ec);
  REQUIRE(p == dirc/"biz");
  ec = bad_ec;
  p = had::WeaklyCanonical(dir/"foo/.././/bar/././baz/.", ec);
  REQUIRE(!ec);
  REQUIRE(p == dirc/"bar/baz");
  ec = bad_ec;
  p = had::WeaklyCanonical(
      stdfs::current_path()/dir/"bar//../foo/../bar/baz", ec);
  REQUIRE(!ec);
  REQUIRE(p == dirc/"bar/baz");

  stdfs::remove_all(dir, ec);
}