extern "C" {
#include "gotcha_mpi_io.h"
}

void init_gotcha_mpi_io() {
  gotcha_wrap(wrap_mpi_io, FUNC_COUNT(wrap_mpi_io), "hermes");
}

int hermes_MPI_Finalize(void) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_Finalize\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function MPI_Finalize()\n");
    fflush(stdout);
  }

  hermes_MPI_Finalize_fp *orig_MPI_Finalize = 
    (hermes_MPI_Finalize_fp *)gotcha_get_wrappee(orig_MPI_Finalize_handle);
  ret = orig_MPI_Finalize();
 
  return ret;
}

int hermes_MPI_Init(int *argc, char ***argv) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_Init\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function MPI_Init()\n");
    fflush(stdout);
  }

  hermes_MPI_Init_fp *orig_MPI_Init = 
    (hermes_MPI_Init_fp *)gotcha_get_wrappee(orig_MPI_Init_handle);
  ret = orig_MPI_Init(argc, argv);

  return ret;
}

int hermes_MPI_File_close(MPI_File *fh) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_File_close\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function MPI_File_close()\n");
    fflush(stdout);
  }

  hermes_MPI_File_close_fp *orig_MPI_File_close = 
    (hermes_MPI_File_close_fp *)gotcha_get_wrappee(orig_MPI_File_close_handle);
  ret = orig_MPI_File_close(fh);

  return ret;
}

int hermes_MPI_File_open(MPI_Comm comm, char *filename, int amode, 
                         MPI_Info info, MPI_File *fh) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_File_open\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function MPI_File_open()\n");
    fflush(stdout);
  }

  hermes_MPI_File_open_fp *orig_hermes_MPI_File_open = 
    (hermes_MPI_File_open_fp *)gotcha_get_wrappee(orig_MPI_File_open_handle);
  ret = orig_hermes_MPI_File_open(comm, filename, amode, info, fh);

  return ret;
}

int hermes_MPI_File_seek(MPI_File fh, MPI_Offset offset, int whence) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_File_seek\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function MPI_File_seek()\n");
    fflush(stdout);
  }
  hermes_MPI_File_seek_fp *orig_hermes_MPI_File_seek =
    (hermes_MPI_File_seek_fp *)gotcha_get_wrappee(orig_MPI_File_seek_handle);
  ret = orig_hermes_MPI_File_seek(fh, offset, whence);

  return ret;
}

int hermes_MPI_File_read(MPI_File fh, void *buf, int count, 
                         MPI_Datatype datatype, MPI_Status *status) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_File_read\n");
  fflush(stdout);

   if (path_match) {
    printf("Intercepting function MPI_File_read()\n");
    fflush(stdout);
  }

  hermes_MPI_File_read_fp *orig_MPI_File_read = 
    (hermes_MPI_File_read_fp *)gotcha_get_wrappee(orig_MPI_File_read_handle);
  ret = orig_MPI_File_read(fh, buf, count, datatype, status);

  return ret;
}

int hermes_MPI_File_write(MPI_File fh, void *buf, int count, 
                          MPI_Datatype datatype, MPI_Status *status) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_File_write\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function MPI_File_write()\n");
    fflush(stdout);
  }

  hermes_MPI_File_write_fp *orig_MPI_File_write = 
    (hermes_MPI_File_write_fp *)gotcha_get_wrappee(orig_MPI_File_write_handle);
  ret = orig_MPI_File_write(fh, buf, count, datatype, status);

  return ret;
}

int hermes_MPI_File_read_at(MPI_File fh, MPI_Offset offset, void *buf, 
                            int count, MPI_Datatype datatype, MPI_Status *status) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_File_read_at\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function MPI_File_read_at()\n");
    fflush(stdout);
  }

  hermes_MPI_File_read_at_fp *orig_MPI_File_read_at = 
    (hermes_MPI_File_read_at_fp *)gotcha_get_wrappee(orig_MPI_File_read_at_handle);
  ret = orig_MPI_File_read_at(fh, offset, buf, count, datatype, status);

  return ret;
}

int hermes_MPI_File_write_at(MPI_File fh, MPI_Offset offset, void *buf, 
                             int count, MPI_Datatype datatype, MPI_Status *status) {
  /** check if path matches prefix */
  bool path_match = 1;
  int ret = 0;

  printf("In wrapper hermes_MPI_File_write_at\n");
  fflush(stdout);

  if (path_match) {
    printf("Intercepting function MPI_File_write_at()\n");
    fflush(stdout);
  }

  hermes_MPI_File_write_at_fp *orig_MPI_File_write_at = 
    (hermes_MPI_File_write_at_fp *)gotcha_get_wrappee(orig_MPI_File_write_at_handle);
  ret = orig_MPI_File_write_at(fh, offset, buf, count, datatype, status);

  return ret;
}

