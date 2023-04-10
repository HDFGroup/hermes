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

#include <string>
#include "hermes_shm/data_structures/ipc/string.h"

template<typename T, bool SHM>
void TestCase() {
  std::string str_type = InternalTypeName<T>::Get();
  /*size_t count = 1000000;
  auto test2 = hipc::make_uptr<hipc::string>();
  auto alloc = test2->GetAllocator();
  size_t x;
  hipc::ShmArchive<T> ar;
  void *ptr_; */

  Timer t;
  t.Resume();
  t.Pause();

  HIPRINT("{},{},{}\n",
          str_type, SHM, t.GetMsec())
}

TEST_CASE("RefBenchmark") {
  TestCase<size_t, false>();
  // TestCase<size_t, true>();
  // TestCase<hipc::string, true>();
}
