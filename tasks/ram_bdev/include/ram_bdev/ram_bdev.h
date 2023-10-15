//
// Created by lukemartinlogan on 6/29/23.
//

#ifndef HRUN_ram_bdev_H_
#define HRUN_ram_bdev_H_

#include "hrun/api/hrun_client.h"
#include "hrun/task_registry/task_lib.h"
#include "hrun_admin/hrun_admin.h"
#include "hrun/queue_manager/queue_manager_client.h"
#include "hermes/hermes_types.h"
#include "bdev/bdev.h"
#include "hrun/hrun_namespace.h"

namespace hermes::ram_bdev {
#include "bdev/bdev_namespace.h"
}  // namespace hrun


#endif  // HRUN_ram_bdev_H_
