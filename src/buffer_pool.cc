#include "buffer_pool.h"

#include <mpi.h>
#include <thallium.hpp>

namespace tl = thallium;

void placeholder() {
  MPI_Init(NULL, NULL);
  tl::engine engine("tcp", THALLIUM_SERVER_MODE);
}
