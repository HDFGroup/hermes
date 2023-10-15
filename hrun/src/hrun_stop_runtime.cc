//
// Created by lukemartinlogan on 6/22/23.
//

#include "hrun_admin/hrun_admin.h"

int main() {
  TRANSPARENT_HRUN();
  HRUN_ADMIN->AsyncStopRuntimeRoot(hrun::DomainId::GetLocal());
}