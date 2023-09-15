//
// Created by lukemartinlogan on 6/22/23.
//

#include "labstor_admin/labstor_admin.h"

int main() {
  TRANSPARENT_LABSTOR();
  LABSTOR_ADMIN->AsyncStopRuntimeRoot(labstor::DomainId::GetLocal());
}