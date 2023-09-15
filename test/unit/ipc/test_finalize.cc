//
// Created by llogan on 7/1/23.
//

#include "basic_test.h"
#include "labstor/api/labstor_client.h"
#include "labstor_admin/labstor_admin.h"

TEST_CASE("TestFinalize") {
  LABSTOR_ADMIN->AsyncStopRuntimeRoot(labstor::DomainId::GetGlobal());
}