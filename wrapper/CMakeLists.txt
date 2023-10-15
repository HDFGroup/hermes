cmake_minimum_required(VERSION 3.10)
project(hermes)

if (HERMES_ENABLE_PYTHON)
    add_subdirectory(python)
endif()
