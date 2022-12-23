//
// Created by lukemartinlogan on 12/22/22.
//

#ifndef HERMES_SRC_IO_CLIENTS_IO_CLIENT_H_
#define HERMES_SRC_IO_CLIENTS_IO_CLIENT_H_

#include "hermes_types.h"
#include "metadata_types.h"

namespace hermes {

class IoClient {
 public:
  virtual bool Write(DeviceInfo &dev_info, void *data,
                     size_t off, size_t size) = 0;
  virtual bool Read(DeviceInfo &dev_info, void *data,
                    size_t off, size_t size) = 0;
};

}  // namespace hermes

#endif  // HERMES_SRC_IO_CLIENTS_IO_CLIENT_H_
