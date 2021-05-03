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

#ifndef HERMES_MPIIO_H
#define HERMES_MPIIO_H

/**
 * Standard header
 */
#include <fcntl.h>
#include <mpi.h>
#include <stdarg.h>
#include <unistd.h>

#include <experimental/filesystem>

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
#include <vbucket.h>

/**
 * Function declaration
 */
HERMES_FORWARD_DECL(MPI_File_close, int, (MPI_File * fh));
HERMES_FORWARD_DECL(MPI_File_iread_at, int,
                    (MPI_File fh, MPI_Offset offset, void *buf, int count,
                     MPI_Datatype datatype, MPI_Request *request));
HERMES_FORWARD_DECL(MPI_File_iread, int,
                    (MPI_File fh, void *buf, int count, MPI_Datatype datatype,
                     MPI_Request *request));
HERMES_FORWARD_DECL(MPI_File_iread_shared, int,
                    (MPI_File fh, void *buf, int count, MPI_Datatype datatype,
                     MPI_Request *request));
HERMES_FORWARD_DECL(MPI_File_iwrite_at, int,
                    (MPI_File fh, MPI_Offset offset, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Request *request));
HERMES_FORWARD_DECL(MPI_File_iwrite, int,
                    (MPI_File fh, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Request *request));
HERMES_FORWARD_DECL(MPI_File_iwrite_shared, int,
                    (MPI_File fh, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Request *request));
HERMES_FORWARD_DECL(MPI_File_open, int,
                    (MPI_Comm comm, const char *filename, int amode,
                     MPI_Info info, MPI_File *fh));
HERMES_FORWARD_DECL(MPI_File_read_all_begin, int,
                    (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
HERMES_FORWARD_DECL(MPI_File_read_all, int,
                    (MPI_File fh, void *buf, int count, MPI_Datatype datatype,
                     MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_read_at_all, int,
                    (MPI_File fh, MPI_Offset offset, void *buf, int count,
                     MPI_Datatype datatype, MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_read_at_all_begin, int,
                    (MPI_File fh, MPI_Offset offset, void *buf, int count,
                     MPI_Datatype datatype));
HERMES_FORWARD_DECL(MPI_File_read_at, int,
                    (MPI_File fh, MPI_Offset offset, void *buf, int count,
                     MPI_Datatype datatype, MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_read, int,
                    (MPI_File fh, void *buf, int count, MPI_Datatype datatype,
                     MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_read_ordered_begin, int,
                    (MPI_File fh, void *buf, int count, MPI_Datatype datatype));
HERMES_FORWARD_DECL(MPI_File_read_ordered, int,
                    (MPI_File fh, void *buf, int count, MPI_Datatype datatype,
                     MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_read_shared, int,
                    (MPI_File fh, void *buf, int count, MPI_Datatype datatype,
                     MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_set_view, int,
                    (MPI_File fh, MPI_Offset disp, MPI_Datatype etype,
                     MPI_Datatype filetype, const char *datarep,
                     MPI_Info info));
HERMES_FORWARD_DECL(MPI_File_sync, int, (MPI_File fh));
HERMES_FORWARD_DECL(MPI_File_write_all_begin, int,
                    (MPI_File fh, const void *buf, int count,
                     MPI_Datatype datatype));
HERMES_FORWARD_DECL(MPI_File_write_all, int,
                    (MPI_File fh, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_write_at_all_begin, int,
                    (MPI_File fh, MPI_Offset offset, const void *buf, int count,
                     MPI_Datatype datatype));
HERMES_FORWARD_DECL(MPI_File_write_at_all, int,
                    (MPI_File fh, MPI_Offset offset, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_write_at, int,
                    (MPI_File fh, MPI_Offset offset, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_write, int,
                    (MPI_File fh, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_write_ordered_begin, int,
                    (MPI_File fh, const void *buf, int count,
                     MPI_Datatype datatype));
HERMES_FORWARD_DECL(MPI_File_write_ordered, int,
                    (MPI_File fh, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Status *status));
HERMES_FORWARD_DECL(MPI_File_write_shared, int,
                    (MPI_File fh, const void *buf, int count,
                     MPI_Datatype datatype, MPI_Status *status));

/**
 * MPI functions declarations
 */
HERMES_FORWARD_DECL(MPI_Init, int, (int *argc, char ***argv));
HERMES_FORWARD_DECL(MPI_Finalize, int, (void));

#endif  // HERMES_MPIIO_H
