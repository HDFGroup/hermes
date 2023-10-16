#------------------------------------------------------------------------------
# Find Packages
#------------------------------------------------------------------------------
# find_package(pybind11 CONFIG REQUIRED)
# message(STATUS "Found pybind11 v${pybind11_VERSION}: ${pybind11_INCLUDE_DIRS}")

#------------------------------------------------------------------------------
# Compile Python Hermes
#------------------------------------------------------------------------------
pybind11_add_module(py_hermes MODULE py_hermes.cpp)
add_dependencies(py_hermes ${Hermes_CLIENT_DEPS} hermes)
target_link_libraries(py_hermes PUBLIC
        ${Hermes_CLIENT_LIBRARIES} hermes pybind11::module)

#------------------------------------------------------------------------------
# Install Targets
#------------------------------------------------------------------------------
install(TARGETS
        py_hermes
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})