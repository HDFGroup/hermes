include_directories(
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_SOURCE_DIR}/src)

#------------------------------------------------------------------------------
# STDIO Adapter Internal tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_mapper_test stdio_adapter_mapper_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_mapper_test stdc++fs hermes_stdio)
add_dependencies(stdio_adapter_mapper_test hermes_stdio)
gcc(stdio_adapter_mapper_test "")

#------------------------------------------------------------------------------
# STDIO Adapter End to End tests
#------------------------------------------------------------------------------
add_executable(stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(stdio_adapter_test hermes glog::glog)
gcc(stdio_adapter_test "")

add_executable(stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
# mpi(stdio_adapter_mpi_test 2 "")

add_executable(hermes_stdio_adapter_test stdio_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_stdio_adapter_test hermes_stdio)
add_dependencies(hermes_stdio_adapter_test hermes_stdio hermes_daemon)
set_target_properties(hermes_stdio_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
gcc_hermes(hermes_stdio_adapter_test "" "~[request_size=range-large]" hermes "")
gcc_hermes(hermes_stdio_adapter_test "large" "[request_size=range-large]" hermes "")
gcc_hermes(hermes_stdio_adapter_test "" "~[request_size=range-large]" hermes async)
gcc_hermes(hermes_stdio_adapter_test "large" "[request_size=range-large]" hermes async)

add_executable(hermes_stdio_low_buf_adapter_test stdio_adapter_low_buffer_space_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_stdio_low_buf_adapter_test hermes_stdio)
add_dependencies(hermes_stdio_low_buf_adapter_test hermes_stdio hermes_daemon)
set_target_properties(hermes_stdio_low_buf_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
# TODO(llogan): should use small buffer hermes config, not default here
gcc_hermes(hermes_stdio_low_buf_adapter_test "" "" hermes "")
gcc_hermes(hermes_stdio_low_buf_adapter_test "" "" hermes async)

add_executable(hermes_stdio_adapter_mode_test stdio_adapter_mode_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_stdio_adapter_mode_test hermes_stdio)
add_dependencies(hermes_stdio_adapter_mode_test hermes_stdio hermes_daemon)
set_target_properties(hermes_stdio_adapter_mode_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
gcc_hermes_mode(hermes_stdio_adapter_mode_test "persistent" "[hermes_mode=persistent]" "DEFAULT" "0")
gcc_hermes_mode(hermes_stdio_adapter_mode_test "scratch" "[hermes_mode=scratch]" "kScratch" "0")
gcc_hermes_mode(hermes_stdio_adapter_mode_test "bypass" "[hermes_mode=bypass]" "kBypass" "0")
gcc_hermes_mode(hermes_stdio_adapter_mode_test "persistent" "[hermes_mode=persistent]" "kDefault" "1")
gcc_hermes_mode(hermes_stdio_adapter_mode_test "scratch" "[hermes_mode=scratch]" "kScratch" "1")
gcc_hermes_mode(hermes_stdio_adapter_mode_test "bypass" "[hermes_mode=bypass]" "kBypass" "1")

add_executable(hermes_stdio_adapter_mpi_test stdio_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_stdio_adapter_mpi_test hermes_stdio)
add_dependencies(hermes_stdio_adapter_mpi_test hermes_stdio hermes_daemon)
set_target_properties(hermes_stdio_adapter_mpi_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
mpi_daemon(hermes_stdio_adapter_mpi_test 2 "~[request_size=range-large]" "")
mpi_daemon(hermes_stdio_adapter_mpi_test 2 "[request_size=range-large]" "large")

set(STDIO_TESTS
  stdio_adapter_mapper_test
  stdio_adapter_test
  hermes_stdio_adapter_test
  hermes_stdio_low_buf_adapter_test
  hermes_stdio_adapter_mode_test
  stdio_adapter_mpi_test
  hermes_stdio_adapter_mpi_test
)

foreach(program ${STDIO_TESTS})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_DIR})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_TEST_DIR})
  target_link_libraries(${program} Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
endforeach()

if(HERMES_INSTALL_TESTS)
    foreach(program ${STDIO_TESTS})
        install(
                TARGETS
                ${program}
                LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
                ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
                RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
        )
    endforeach()
endif()
