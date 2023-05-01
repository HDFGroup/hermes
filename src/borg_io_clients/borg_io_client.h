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

#ifndef HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_H
#define HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_H

#include "hermes_types.h"
#include "metadata_types.h"

namespace hermes {

class BorgIoClient {
 public:
  virtual ~BorgIoClient() = default;
  virtual bool Init(DeviceInfo &dev_info) = 0;
  virtual bool Write(DeviceInfo &dev_info,
                     const char *data, size_t off, size_t size) = 0;
  virtual bool Read(DeviceInfo &dev_info,
                    char *data, size_t off, size_t size) = 0;
};

}  // namespace hermes

#endif  // HERMES_SRC_BORG_IO_CLIENTS_IO_CLIENT_H
