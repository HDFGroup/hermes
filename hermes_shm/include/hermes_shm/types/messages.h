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

#ifndef HERMES_SHM_MESSAGES_H
#define HERMES_SHM_MESSAGES_H

namespace hermes_shm {

enum {
  HERMES_SHM_ADMIN_REGISTER_QP
};

struct admin_request {
  int op_;
  admin_request() {}
  explicit admin_request(int op) : op_(op) {}
};

struct admin_reply {
  int code_;
  admin_reply() {}
  explicit admin_reply(int code) : code_(code) {}
};

struct setup_reply : public admin_reply {
  uint32_t region_id_;
  uint32_t region_size_;
  uint32_t request_region_size_;
  uint32_t request_unit_;
  uint32_t queue_region_size_;
  uint32_t queue_depth_;
  uint32_t num_queues_;
  uint32_t namespace_region_id_;
  uint32_t namespace_region_size_;
  uint32_t namespace_max_entries_;
};

}  // namespace hermes_shm

#endif  // HERMES_SHM_MESSAGES_H
