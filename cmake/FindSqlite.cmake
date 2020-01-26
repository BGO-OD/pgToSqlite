# - Try to find Sqlite
# Once done this will define
#  SQLITE_FOUND - System has Sqlite
#  SQLITE_INCLUDE_DIR - The Sqlite include directory
#  SQLITE_LIBRARIES - The libraries needed to use Sqlite

find_package(PkgConfig)
pkg_check_modules(PC_SQLITE QUIET sqlite3)

find_path(SQLITE_INCLUDE_DIR NAMES sqlite3.h
  HINTS ${PC_SQLITE_INCLUDEDIR} ${PC_SQLITE_INCLUDE_DIRS} )

find_library(SQLITE_LIBRARIES NAMES sqlite3
  HINTS ${PC_SQLITE_LIBDIR} ${PC_SQLITE_LIBRARY_DIRS} )

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Sqlite DEFAULT_MSG SQLITE_INCLUDE_DIR SQLITE_LIBRARIES )

mark_as_advanced(SQLITE_INCLUDE_DIR SQLITE_LIBRARIES)
