set(PUBSUB_TESTS pubsub_connection_test pubsub_topic_test
        pubsub_end_to_end_test pubsub_end_to_end_test_sync)

add_executable(pubsub_metadata_test ${CMAKE_CURRENT_SOURCE_DIR}/pubsub_metadata_test.cc)
add_test(NAME "pubsub_metadata_test" COMMAND "${CMAKE_BINARY_DIR}/bin/pubsub_metadata_test")
target_link_libraries(pubsub_metadata_test ${LIBRT} hermes MPI::MPI_CXX
        $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> hermes_pubsub)
add_dependencies(pubsub_metadata_test hermes_pubsub)

add_executable(pubsub_negative_connection_test ${CMAKE_CURRENT_SOURCE_DIR}/pubsub_negative_connection_test.cc)
add_test(NAME "pubsub_negative_connection_test" COMMAND "${CMAKE_BINARY_DIR}/bin/pubsub_negative_connection_test")
target_link_libraries(pubsub_metadata_test ${LIBRT} hermes MPI::MPI_CXX
        $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> hermes_pubsub)
add_dependencies(pubsub_negative_connection_test hermes_pubsub)

foreach(program ${PUBSUB_TESTS})
    add_executable(${program} ${CMAKE_CURRENT_SOURCE_DIR}/${program}.cc)
    add_dependencies(${program} hermes_pubsub)
    target_link_libraries(${program} ${LIBRT} hermes MPI::MPI_CXX
            $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium> hermes_pubsub)
    target_compile_definitions(${program}
            PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
    mpi_daemon(${program} 1 "" "" 1)
endforeach()

#add_custom_target(pubsub_ares
#        COMMAND LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp
#        ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 -ppn 2
#        "${CMAKE_BINARY_DIR}/bin/end_to_end_test"
#        "${CMAKE_CURRENT_SOURCE_DIR}/data/ares.conf"
#        )

if(HERMES_INSTALL_TESTS)
    set(PUBSUB_TESTS pubsub_connection_test pubsub_metadata_test pubsub_negative_connection_test pubsub_topic_test
            pubsub_end_to_end_test pubsub_end_to_end_test_sync)
    foreach(program ${PUBSUB_TESTS})
        install(
                TARGETS
                ${program}
                EXPORT
                ${HERMES_EXPORTED_TARGETS}
                LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
                ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
                RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR}
        )
    endforeach()
endif()