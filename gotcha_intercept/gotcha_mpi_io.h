#ifndef GOTCHA_MPI_IO_H_
#define GOTCHA_MPI_IO_H_

#include <stdio.h>
#include <mpi.h>

#include "gotcha/gotcha.h"
#include "gotcha/gotcha_types.h"

#define FUNC_COUNT(wraps) ((sizeof(wraps) / sizeof(wraps[0])))

gotcha_wrappee_handle_t orig_MPI_Finalize_handle;
typedef int (hermes_MPI_Finalize_fp)(void);
int hermes_MPI_Finalize(void);

gotcha_wrappee_handle_t orig_MPI_Init_handle;
typedef int (hermes_MPI_Init_fp)(int *argc, char ***argv);
int hermes_MPI_Init(int *argc, char ***argv);

gotcha_wrappee_handle_t orig_MPI_File_close_handle;
typedef int (hermes_MPI_File_close_fp)(MPI_File *fh);
int hermes_MPI_File_close(MPI_File *fh);

gotcha_wrappee_handle_t orig_MPI_File_open_handle;
typedef int (hermes_MPI_File_open_fp)(MPI_Comm comm, char *filename, int amode, 
                                      MPI_Info info, MPI_File *fh);
int hermes_MPI_File_open(MPI_Comm comm, char *filename, int amode, MPI_Info info, 
                         MPI_File *fh);

gotcha_wrappee_handle_t orig_MPI_File_seek_handle;
typedef int (hermes_MPI_File_seek_fp)(MPI_File fh, MPI_Offset offset, int whence);
int hermes_MPI_File_seek(MPI_File fh, MPI_Offset offset, int whence);

gotcha_wrappee_handle_t orig_MPI_File_read_handle;
typedef int (hermes_MPI_File_read_fp)(MPI_File fh, void *buf, int count,
                                   MPI_Datatype datatype, MPI_Status *status);
int hermes_MPI_File_read(MPI_File fh, void *buf, int count, MPI_Datatype datatype, 
                         MPI_Status *status);

gotcha_wrappee_handle_t orig_MPI_File_write_handle;
typedef int (hermes_MPI_File_write_fp)(MPI_File fh, void *buf, int count,
                                       MPI_Datatype datatype, MPI_Status *status);
int hermes_MPI_File_write(MPI_File fh, void *buf, int count, MPI_Datatype datatype, 
                          MPI_Status *status);

gotcha_wrappee_handle_t orig_MPI_File_read_at_handle;
typedef int (hermes_MPI_File_read_at_fp)(MPI_File fh, MPI_Offset offset, void *buf, 
                                         int count, MPI_Datatype datatype, 
                                         MPI_Status *status);
int hermes_MPI_File_read_at(MPI_File fh, MPI_Offset offset, void *buf, 
                            int count, MPI_Datatype datatype, MPI_Status *status);

gotcha_wrappee_handle_t orig_MPI_File_write_at_handle;
typedef int (hermes_MPI_File_write_at_fp)(MPI_File fh, MPI_Offset offset, void *buf,
                                          int count, MPI_Datatype datatype, 
                                          MPI_Status *status);
int hermes_MPI_File_write_at(MPI_File fh, MPI_Offset offset, void *buf, 
                             int count, MPI_Datatype datatype, MPI_Status *status);

struct gotcha_binding_t wrap_mpi_io [] = {
	{ "MPI_Finalize", (void *)hermes_MPI_Finalize, &orig_MPI_Finalize_handle },
	{ "MPI_Init", (void *)hermes_MPI_Init, &orig_MPI_Init_handle },
	{ "MPI_File_close", (void *)hermes_MPI_File_close, &orig_MPI_File_close_handle },
	{ "MPI_File_open", (void *)hermes_MPI_File_open, &orig_MPI_File_open_handle },
    { "MPI_File_seek", (void *)hermes_MPI_File_seek, &orig_MPI_File_seek_handle },
	{ "MPI_File_read", (void *)hermes_MPI_File_read, &orig_MPI_File_read_handle },
	{ "MPI_File_write", (void *)hermes_MPI_File_write, &orig_MPI_File_write_handle },
    { "MPI_File_read_at", (void *)hermes_MPI_File_read_at, &orig_MPI_File_read_at_handle },
    { "MPI_File_write_at", (void *)hermes_MPI_File_write_at, &orig_MPI_File_write_at_handle },
};

void init_gotcha_mpi_io ();

#endif  // GOTCHA_MPI_IO_H_
