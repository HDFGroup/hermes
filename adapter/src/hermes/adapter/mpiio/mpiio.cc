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

#include <hermes/adapter/mpiio.h>

/**
 * Metadata functions
 */
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_open)(MPI_Comm comm, const char *filename, int amode,
                                MPI_Info info, MPI_File *fh)
#else
int HERMES_DECL(PMPI_File_open)(MPI_Comm comm, char *filename, int amode,
                                MPI_Info info, MPI_File *fh)
#endif
{
  (void)comm;
  (void)filename;
  (void)amode;
  (void)info;
  (void)fh;
  return 0;
}

int HERMES_DECL(PMPI_File_close)(MPI_File *fh) {
  (void)fh;
  return 0;
}

#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_set_view)(MPI_File fh, MPI_Offset disp,
                                    MPI_Datatype etype, MPI_Datatype filetype,
                                    const char *datarep, MPI_Info info)
#else
int HERMES_DECL(PMPI_File_set_view)(MPI_File fh, MPI_Offset disp,
                                    MPI_Datatype etype, MPI_Datatype filetype,
                                    char *datarep, MPI_Info info)
#endif
{
  (void)fh;
  (void)disp;
  (void)etype;
  (void)filetype;
  (void)datarep;
  (void)info;
  return 0;
}

/**
 * Sync Read/Write
 */
int HERMES_DECL(PMPI_File_read_all_begin)(MPI_File fh, void *buf, int count,
                                          MPI_Datatype datatype) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
int HERMES_DECL(PMPI_File_read_all)(MPI_File fh, void *buf, int count,
                                    MPI_Datatype datatype, MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(PMPI_File_read_at_all)(MPI_File fh, MPI_Offset offset,
                                       void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  (void)offset;
  return 0;
}
int HERMES_DECL(PMPI_File_read_at_all_begin)(MPI_File fh, MPI_Offset offset,
                                             void *buf, int count,
                                             MPI_Datatype datatype) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)offset;
  return 0;
}
int HERMES_DECL(PMPI_File_read_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                   int count, MPI_Datatype datatype,
                                   MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  (void)offset;
  return 0;
}
int HERMES_DECL(PMPI_File_read)(MPI_File fh, void *buf, int count,
                                MPI_Datatype datatype, MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(PMPI_File_read_ordered_begin)(MPI_File fh, void *buf, int count,
                                              MPI_Datatype datatype) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
int HERMES_DECL(PMPI_File_read_ordered)(MPI_File fh, void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
int HERMES_DECL(PMPI_File_read_shared)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype,
                                       MPI_Status *status) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write_all_begin)(MPI_File fh, const void *buf,
                                           int count, MPI_Datatype datatype)
#else
int HERMES_DECL(PMPI_File_write_all_begin)(MPI_File fh, void *buf, int count,
                                           MPI_Datatype datatype)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write_all)(MPI_File fh, const void *buf, int count,
                                     MPI_Datatype datatype, MPI_Status *status)
#else
int HERMES_DECL(PMPI_File_write_all)(MPI_File fh, void *buf, int count,
                                     MPI_Datatype datatype, MPI_Status *status)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write_at_all_begin)(MPI_File fh, MPI_Offset offset,
                                              const void *buf, int count,
                                              MPI_Datatype datatype)
#else
int HERMES_DECL(PMPI_File_write_at_all_begin)(MPI_File fh, MPI_Offset offset,
                                              void *buf, int count,
                                              MPI_Datatype datatype)
#endif
{
  (void)fh;
  (void)offset;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write_at_all)(MPI_File fh, MPI_Offset offset,
                                        const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status)
#else
int HERMES_DECL(PMPI_File_write_at_all)(MPI_File fh, MPI_Offset offset,
                                        void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  (void)offset;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write_at)(MPI_File fh, MPI_Offset offset,
                                    const void *buf, int count,
                                    MPI_Datatype datatype, MPI_Status *status)
#else
int HERMES_DECL(PMPI_File_write_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                    int count, MPI_Datatype datatype,
                                    MPI_Status *status)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  (void)offset;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write)(MPI_File fh, const void *buf, int count,
                                 MPI_Datatype datatype, MPI_Status *status)
#else
int HERMES_DECL(PMPI_File_write)(MPI_File fh, void *buf, int count,
                                 MPI_Datatype datatype, MPI_Status *status)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write_ordered_begin)(MPI_File fh, const void *buf,
                                               int count, MPI_Datatype datatype)
#else
int HERMES_DECL(PMPI_File_write_ordered_begin)(MPI_File fh, void *buf,
                                               int count, MPI_Datatype datatype)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write_ordered)(MPI_File fh, const void *buf,
                                         int count, MPI_Datatype datatype,
                                         MPI_Status *status)
#else
int HERMES_DECL(PMPI_File_write_ordered)(MPI_File fh, void *buf, int count,
                                         MPI_Datatype datatype,
                                         MPI_Status *status)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_write_shared)(MPI_File fh, const void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status)
#else
int HERMES_DECL(PMPI_File_write_shared)(MPI_File fh, void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Status *status)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)status;
  return 0;
}
/**
 * Async Read/Write
 */
int HERMES_DECL(PMPI_File_iread_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                    int count, MPI_Datatype datatype,
                                    MPI_Request *request) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  (void)offset;
  return 0;
}
int HERMES_DECL(PMPI_File_iread)(MPI_File fh, void *buf, int count,
                                 MPI_Datatype datatype, MPI_Request *request) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  return 0;
}
int HERMES_DECL(PMPI_File_iread_shared)(MPI_File fh, void *buf, int count,
                                        MPI_Datatype datatype,
                                        MPI_Request *request) {
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset,
                                     const void *buf, int count,
                                     MPI_Datatype datatype,
                                     MPI_Request *request)
#else
int HERMES_DECL(PMPI_File_iwrite_at)(MPI_File fh, MPI_Offset offset, void *buf,
                                     int count, MPI_Datatype datatype,
                                     MPI_Request *request)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  (void)offset;
  return 0;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_iwrite)(MPI_File fh, const void *buf, int count,
                                  MPI_Datatype datatype, MPI_Request *request)
#else
int HERMES_DECL(PMPI_File_iwrite)(MPI_File fh, void *buf, int count,
                                  MPI_Datatype datatype, MPI_Request *request)
#endif
{
  return 0;
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
}
#ifdef HAVE_MPI_CONST
int HERMES_DECL(PMPI_File_iwrite_shared)(MPI_File fh, const void *buf,
                                         int count, MPI_Datatype datatype,
                                         MPI_Request *request)
#else
int HERMES_DECL(PMPI_File_iwrite_shared)(MPI_File fh, void *buf, int count,
                                         MPI_Datatype datatype,
                                         MPI_Request *request)
#endif
{
  (void)fh;
  (void)buf;
  (void)count;
  (void)datatype;
  (void)request;
  return 0;
}

/**
 * Other functions
 */
int HERMES_DECL(PMPI_File_sync)(MPI_File fh) {
  (void)fh;
  return 0;
}
