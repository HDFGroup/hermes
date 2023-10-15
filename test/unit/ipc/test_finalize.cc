//
// Created by llogan on 7/1/23.
//

#include "basic_test.h"
#include "hrun/api/hrun_client.h"
#include "hrun_admin/hrun_admin.h"

TEST_CASE("TestFinalize") {
  HRUN_ADMIN->AsyncStopRuntimeRoot(hrun::DomainId::GetGlobal());
}