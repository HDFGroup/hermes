#include <catch_config.h>
#include <hermes/adapter/posix.h>
#include <iostream>
#include <filesystem>
#include <unistd.h>
#include <fcntl.h>
#include <fstream>

namespace fs = std::filesystem;

namespace hermes::adapter::posix::test{
    struct Arguments{
        std::string filename = "test.dat";
        std::string directory = "/tmp";
        long request_size = 4096;
    };
    struct Info{
        std::string data;
    };
}
hermes::adapter::posix::test::Arguments args;
hermes::adapter::posix::test::Info info;

int init(){
    info.data = std::string(args.request_size,'x');
    return 0;
}
int finalize(){
    return 0;
}

Parser define_options() {
    return Opt(args.filename, "filename" )["-f"]["--filename"]("Filename used for performing I/O")
           | Opt( args.directory, "dir" )["-d"]["--directory"]("Directory used for performing I/O")
           | Opt( args.request_size, "request_size" )["-s"]["--request_size"]("Request size used for performing I/O");
}

TEST_CASE("Open", "[process=1][operation=single_write][request_size=type-fixed]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    std::string existing_file = fullpath.string() + "_ext";
    if(fs::exists(new_file)) fs::remove(new_file);
    if(fs::exists(existing_file)) fs::remove(existing_file);
    if(!fs::exists(existing_file)){
        std::ofstream ofs(existing_file);
        ofs.close();
    }

    SECTION( "write to an non-existant file" ) {
        int fd = open(new_file.c_str(),O_WRONLY);
        REQUIRE( fd == -1 );
    }

    SECTION( "create a new file" ) {
        int fd = open(new_file.c_str(),O_WRONLY | O_CREAT);
        REQUIRE( fd != -1 );
        int status = close(fd);
        REQUIRE( status == 0 );
    }

    SECTION( "open existing file" ) {
        int fd = open(existing_file.c_str(),O_WRONLY);
        REQUIRE( fd != -1 );
        int status = close(fd);
        REQUIRE( status == 0 );
    }

    SECTION( "create existing file" ) {
        int fd = open(existing_file.c_str(),O_CREAT | O_EXCL);
        REQUIRE( fd == -1 );
    }
    fs::remove(fullpath);
}

TEST_CASE("Write", "[process=1][operation=single_write][request_size=type-fixed]") {
    fs::path fullpath = args.directory;
    fullpath /= args.filename;
    std::string new_file = fullpath.string() + "_new";
    std::string existing_file = fullpath.string() + "_ext";
    if(fs::exists(new_file)) fs::remove(new_file);
    if(fs::exists(existing_file)) fs::remove(existing_file);
    if(!fs::exists(existing_file)){
        std::ofstream ofs(existing_file);
        ofs.close();
    }

    SECTION( "write to existing file" ) {
        int fd = open(existing_file.c_str(),O_WRONLY);
        REQUIRE( fd != -1 );
        long offset = lseek(fd,0,SEEK_SET);
        long size_written = write(fd,info.data.c_str(),args.request_size);
        REQUIRE( size_written == args.request_size );
        int status = close(fd);
        REQUIRE( status == 0 );
        REQUIRE(fs::file_size(existing_file) == size_written);
    }

    SECTION( "write to new  file" ) {
        int fd = open(new_file.c_str(),O_WRONLY | O_CREAT);
        REQUIRE( fd != -1 );
        long size_written = write(fd,info.data.c_str(),args.request_size);
        REQUIRE( size_written == args.request_size );
        int status = close(fd);
        REQUIRE( status == 0 );
        REQUIRE(fs::file_size(new_file) == size_written);
    }

    SECTION( "write to existing file with truncate" ) {
        int fd = open(existing_file.c_str(),O_WRONLY | O_CREAT | O_TRUNC);
        REQUIRE( fd != -1 );
        long size_written = write(fd,info.data.c_str(),args.request_size);
        REQUIRE( size_written == args.request_size );
        int status = close(fd);
        REQUIRE( status == 0 );
        REQUIRE(fs::file_size(existing_file) == size_written);
    }

    SECTION( "append to existing file" ) {
        auto existing_size = fs::file_size(existing_file);
        int fd = open(existing_file.c_str(),O_WRONLY|O_APPEND);
        REQUIRE( fd != -1 );
        long size_written = write(fd,info.data.c_str(),args.request_size);
        REQUIRE( size_written == args.request_size );
        int status = close(fd);
        REQUIRE( status == 0 );
        REQUIRE(fs::file_size(existing_file) == existing_size + size_written);
    }

    SECTION( "append to new file" ) {
        int fd = open(new_file.c_str(),O_WRONLY| O_APPEND | O_CREAT);
        REQUIRE( fd != -1 );
        long size_written = write(fd,info.data.c_str(),args.request_size);
        REQUIRE( size_written == args.request_size );
        int status = close(fd);
        REQUIRE( status == 0 );
        REQUIRE(fs::file_size(new_file) == size_written);
    }
    fs::remove(fullpath);
}