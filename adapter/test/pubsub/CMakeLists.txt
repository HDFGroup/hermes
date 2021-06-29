set(PUBSUB_TESTS pubsub_connection_test pubsub_metadata_test pubsub_negative_connection_test pubsub_topic_test
        pubsub_end_to_end_test)

foreach(program ${PUBSUB_TESTS})
    add_executable(${program} ${CMAKE_CURRENT_SOURCE_DIR}/${program}.cc pubsub_metadata_test.cc pubsub_connection_test.cc pubsub_negative_connection_test.cc pubsub_topic_test.cc)
    target_link_libraries(${program} ${LIBRT} hermes MPI::MPI_CXX
            $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)
    target_compile_definitions(${program}
            PRIVATE $<$<BOOL:${HERMES_RPC_THALLIUM}>:HERMES_RPC_THALLIUM>)
    add_test(NAME "Test${program}"
            COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2
            "${CMAKE_BINARY_DIR}/bin/${program}")
    set_tests_properties("Test${program}" PROPERTIES ENVIRONMENT
            LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp)
endforeach()

#add_custom_target(pubsub_ares
#        COMMAND LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/data/asan.supp
#        ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 -ppn 2
#        "${CMAKE_BINARY_DIR}/bin/end_to_end_test"
#        "${CMAKE_CURRENT_SOURCE_DIR}/data/ares.conf"
#        )

if(HERMES_INSTALL_TESTS)
    set(PUBSUB_TESTS pubsub_connection_test pubsub_metadata_test pubsub_negative_connection_test pubsub_topic_test
            pubsub_end_to_end_test)
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