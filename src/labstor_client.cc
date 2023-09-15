//
// Created by lukemartinlogan on 6/27/23.
//

#include "hermes_shm/util/singleton.h"
#include "labstor/api/labstor_client.h"

/** Runtime singleton */
DEFINE_SINGLETON_CC(labstor::Client)