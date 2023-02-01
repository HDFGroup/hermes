//
// Created by lukemartinlogan on 12/22/22.
//

#ifndef HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_H
#define HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_H

#include "hermes_types.h"
#include "metadata_types.h"

namespace hermes {

class BorgIoClient {
 public:
  virtual bool Init(DeviceInfo &dev_info) = 0;
  virtual bool Write(DeviceInfo &dev_info,
                     const char *data, size_t off, size_t size) = 0;
  virtual bool Read(DeviceInfo &dev_info,
                    char *data, size_t off, size_t size) = 0;
};

}  // namespace hermes

#endif  // HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_H
