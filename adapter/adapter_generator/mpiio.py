from adapter_generator.create_interceptor import Api,ApiClass

apis = [
    Api("int MPI_Init(int *argc, char ***argv)"),
    Api("int MPI_Finalize(void)"),
    Api("int MPI_Wait(MPI_Request *req, MPI_Status *status)"),
    Api("int MPI_Waitall(int count, MPI_Request *req, MPI_Status *status)"),
    Api("int MPI_File_open(MPI_Comm comm, const char *filename, int amode, MPI_Info info, MPI_File *fh)"),
    Api("int MPI_File_close(MPI_File *fh)"),
    Api("int MPI_File_seek_shared(MPI_File fh, MPI_Offset offset, int whence)"),
    Api("int MPI_File_seek(MPI_File fh, MPI_Offset offset, int whence)"),
    Api("int MPI_File_get_position(MPI_File fh, MPI_Offset *offset)"),
    Api("int MPI_File_read_all(MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_read_at_all(MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_read_at(MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_read(MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_read_ordered(MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_read_shared(MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_write_all(MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_write_at_all(MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_write_at(MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_write(MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_write_ordered(MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_write_shared(MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Status *status)"),
    Api("int MPI_File_iread_at(MPI_File fh, MPI_Offset offset, void *buf, int count, MPI_Datatype datatype, MPI_Request *request)"),
    Api("int MPI_File_iread(MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Request *request)"),
    Api("int MPI_File_iread_shared(MPI_File fh, void *buf, int count, MPI_Datatype datatype, MPI_Request *request)"),
    Api("int MPI_File_iwrite_at(MPI_File fh, MPI_Offset offset, const void *buf, int count, MPI_Datatype datatype, MPI_Request *request)"),
    Api("int MPI_File_iwrite(MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Request *request)"),
    Api("int MPI_File_iwrite_shared(MPI_File fh, const void *buf, int count, MPI_Datatype datatype, MPI_Request *request)"),
    Api("int MPI_File_sync(MPI_File fh)"),
]

includes = [
    "#include <mpi.h>",
    "#include <mpio.h>"
]

ApiClass("mpiio", apis, includes)