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

//
// Created by hdevarajan on 12/7/20.
//

#ifndef HERMES_ADAPTER_POSIX_H
#define HERMES_ADAPTER_POSIX_H
/**
 * Standard header
 */
#include <fcntl.h>
#include <stdarg.h>
/**
 * Dependent library headers
 */
#include "glog/logging.h"
/**
 * Internal headers
 */
#include <bucket.h>
#include <hermes.h>
#include <hermes/adapter/interceptor.h>
#include <hermes/adapter/singleton.h>
#include <hermes/adapter/posix/common/constants.h>
#include <hermes/adapter/posix/common/datastructures.h>
#include <hermes/adapter/posix/mapper/mapper_factory.h>
#include <hermes/adapter/posix/metadata_manager.h>
#include <vbucket.h>

/**
 * Function declarations
 */
HERMES_FORWARD_DECL(open, int, (const char *path, int flags, ...));
HERMES_FORWARD_DECL(open64, int, (const char *path, int flags, ...));
HERMES_FORWARD_DECL(__open_2, int, (const char *path, int oflag));
HERMES_FORWARD_DECL(creat, int, (const char* path, mode_t mode));
HERMES_FORWARD_DECL(creat64, int, (const char* path, mode_t mode));
HERMES_FORWARD_DECL(read, ssize_t, (int fd, void *buf, size_t count));
HERMES_FORWARD_DECL(write, ssize_t, (int fd, const void *buf, size_t count));
HERMES_FORWARD_DECL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
HERMES_FORWARD_DECL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
HERMES_FORWARD_DECL(pread64, ssize_t, (int fd, void *buf, size_t count, off64_t offset));
HERMES_FORWARD_DECL(pwrite64, ssize_t, (int fd, const void *buf, size_t count, off64_t offset));
HERMES_FORWARD_DECL(lseek, off_t, (int fd, off_t offset, int whence));
HERMES_FORWARD_DECL(lseek64, off64_t, (int fd, off64_t offset, int whence));
HERMES_FORWARD_DECL(fsync, int, (int fd));
HERMES_FORWARD_DECL(fdatasync, int, (int fd));
HERMES_FORWARD_DECL(close, int, (int fd));
/**
 * MPI functions declarations
 */
HERMES_FORWARD_DECL(MPI_Init, int, (int *argc, char ***argv));
HERMES_FORWARD_DECL(MPI_Finalize, int, (void));
#endif  // HERMES_ADAPTER_POSIX_H
