function(gcc_hermes exec tag_name tags conf)
    add_test(NAME Test${exec}_${tag_name} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${tags} --reporter compact -d yes)
    set_property(TEST Test${exec}_${tag_name}
            PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/${conf}.conf)
    set_property(TEST Test${exec}_${tag_name} APPEND
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
endfunction()

function(gcc_hermes_mode exec tag_name tags mode path)
    set(test_name Test${exec}_${tag_name}_${mode}_${path})
    add_test(NAME ${test_name} COMMAND "${CMAKE_BINARY_DIR}/bin/${exec}" ${tags} --reporter compact -d yes)
    set_property(TEST ${test_name}
            PROPERTY ENVIRONMENT HERMES_CONF=${CMAKE_SOURCE_DIR}/adapter/test/data/hermes.conf)
    set_property(TEST ${test_name} APPEND
            PROPERTY ENVIRONMENT LSAN_OPTIONS=suppressions=${CMAKE_SOURCE_DIR}/test/data/asan.supp)
    set_property(TEST ${test_name} APPEND
            PROPERTY ENVIRONMENT ADAPTER_MODE=${mode})
    set_property(TEST ${test_name} APPEND
            PROPERTY ENVIRONMENT SET_PATH=${path})
endfunction()

#------------------------------------------------------------------------------
# Posix Adapter tests
#------------------------------------------------------------------------------

add_executable(posix_adapter_test posix_adapter_test.cpp ${ADAPTER_COMMON})
gcc(posix_adapter_test "")

add_executable(posix_adapter_mpi_test posix_adapter_mpi_test.cpp ${ADAPTER_COMMON})
mpi(posix_adapter_mpi_test 2 "")

add_executable(hermes_posix_adapter_test posix_adapter_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_posix_adapter_test hermes_posix)
add_dependencies(hermes_posix_adapter_test hermes_posix hermes_daemon)
set_target_properties(hermes_posix_adapter_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
gcc_hermes(hermes_posix_adapter_test "" "~[request_size=range-large]" hermes)
gcc_hermes(hermes_posix_adapter_test "large" "[request_size=range-large]" hermes)

add_executable(hermes_posix_adapter_mpi_test posix_adapter_mpi_test.cpp ${ADAPTER_COMMON})
target_link_libraries(hermes_posix_adapter_mpi_test hermes_posix)
add_dependencies(hermes_posix_adapter_mpi_test hermes_posix hermes_daemon)
set_target_properties(hermes_posix_adapter_mpi_test PROPERTIES COMPILE_FLAGS "-DHERMES_INTERCEPT=1")
mpi_daemon(hermes_posix_adapter_mpi_test 2 "~[request_size=range-large]" "" 1)
mpi_daemon(hermes_posix_adapter_mpi_test 2 "[request_size=range-large]" "large" 1)

set(POSIX_TESTS
  posix_adapter_test
  hermes_posix_adapter_test
  posix_adapter_mpi_test
  hermes_posix_adapter_mpi_test
)

foreach(program ${POSIX_TESTS})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_DIR})
  target_include_directories(${program} PRIVATE ${HERMES_ADAPTER_TEST_DIR})
  target_link_libraries(${program} Catch2::Catch2 -lstdc++fs -lc MPI::MPI_CXX)
endforeach()

if(HERMES_INSTALL_TESTS)
    foreach(program ${POSIX_TESTS})
        install(
                TARGETS
                ${program}
                LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
                ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
                RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
        )
    endforeach()
endif()
