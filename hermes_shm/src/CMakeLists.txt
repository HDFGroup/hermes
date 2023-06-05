cmake_minimum_required(VERSION 3.10)
project(hermes_shm)

set(CMAKE_CXX_STANDARD 17)

#-----------------------------------------------------------------------------
# Build HSHM
#-----------------------------------------------------------------------------
add_library(hermes_shm_data_structures SHARED
        thread/mutex.cc
        thread/rwlock.cc
        memory/malloc_allocator.cc
        memory/stack_allocator.cc
        memory/scalable_page_allocator.cc
        memory/memory_registry.cc
        memory/memory_manager.cc
        data_structure_singleton.cc)
target_link_libraries(hermes_shm_data_structures
        yaml-cpp pthread -lrt -ldl OpenMP::OpenMP_CXX
        $<$<BOOL:${HERMES_RPC_THALLIUM}>:thallium>)

#-----------------------------------------------------------------------------
# Add Target(s) to CMake Install
#-----------------------------------------------------------------------------
install(TARGETS
        hermes_shm_data_structures
        EXPORT
        ${HERMES_EXPORTED_TARGETS}
        LIBRARY DESTINATION ${HERMES_INSTALL_LIB_DIR}
        ARCHIVE DESTINATION ${HERMES_INSTALL_LIB_DIR}
        RUNTIME DESTINATION ${HERMES_INSTALL_BIN_DIR})

#-----------------------------------------------------------------------------
# Export all exported targets to the build tree for use by parent project
#-----------------------------------------------------------------------------
set(HERMES_EXPORTED_LIBS
        hermes_shm_data_structures
        ${HERMES_EXPORTED_LIBS})
if(NOT HERMES_EXTERNALLY_CONFIGURED)
    EXPORT (
            TARGETS
            ${HERMES_EXPORTED_LIBS}
            FILE
            ${HERMES_EXPORTED_TARGETS}.cmake
    )
endif()

#-----------------------------------------------------------------------------
# Coverage
#-----------------------------------------------------------------------------
if(HERMES_ENABLE_COVERAGE)
    set_coverage_flags(hermes_shm_data_structures)
endif()

