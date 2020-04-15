# - Try to find libgotcha
# Once done this will define
#  GOTCHA_FOUND - System has gotcha
#  GOTCHA_INCLUDE_DIRS - The gotcha include directories
#  GOTCHA_LIBRARIES - The libraries needed to use gotcha

set(_GOTCHA_SEARCHES ${CMAKE_PREFIX_PATH})

if(GOTCHA_ROOT)
  set(_GOTCHA_SEARCH_ROOT PATHS ${GOTCHA_ROOT} NO_DEFAULT_PATH)
  list(APPEND _GOTCHA_SEARCHES _GOTCHA_SEARCH_ROOT)
endif()

if(GOTCHA_DIR)
  set(_GOTCHA_SEARCH_ROOT PATHS ${GOTCHA_DIR} NO_DEFAULT_PATH)
  list(APPEND _GOTCHA_SEARCHES _GOTCHA_SEARCH_ROOT)
endif()

find_path(GOTCHA_INCLUDE_DIRS gotcha/gotcha.h 
  ${_GOTCHA_SEARCHES} PATH_SUFFIXES include
)

find_library(GOTCHA_LIBRARIES NAMES gotcha 
  ${_GOTCHA_SEARCHES} PATH_SUFFIXES lib lib64
)

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set GOTCHA_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(
  GOTCHA DEFAULT_MSG
  GOTCHA_LIBRARIES GOTCHA_INCLUDE_DIRS)

mark_as_advanced(GOTCHA_LIBRARIES GOTCHA_INCLUDE_DIRS)
