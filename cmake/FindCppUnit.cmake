#
# From: https://gitlab.cern.ch/gaudi/Gaudi/blob/master/cmake/modules/FindCppUnit.cmake
#
# - Locate CppUnit library
# Defines:
#
#  CPPUNIT_FOUND
#  CPPUNIT_INCLUDE_DIR
#  CPPUNIT_INCLUDE_DIRS (not cached)
#  CPPUNIT_LIBRARY
#  CPPUNIT_LIBRARIES (not cached)

find_path(CPPUNIT_INCLUDE_DIR cppunit/Test.h)
find_library(CPPUNIT_LIBRARY NAMES cppunit)

set(CPPUNIT_INCLUDE_DIRS ${CPPUNIT_INCLUDE_DIR})

# handle the QUIETLY and REQUIRED arguments and set CPPUNIT_FOUND to TRUE if
# all listed variables are TRUE
INCLUDE(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(CppUnit DEFAULT_MSG CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARY)

mark_as_advanced(CPPUNIT_FOUND CPPUNIT_INCLUDE_DIR CPPUNIT_LIBRARIES)

set(CPPUNIT_LIBRARIES ${CPPUNIT_LIBRARY} ${CMAKE_DL_LIBS})
