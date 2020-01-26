# Check if cmake has the required version
CMAKE_MINIMUM_REQUIRED(VERSION 2.6.0 FATAL_ERROR)

# Set project name
PROJECT(pgToSqlite)

SET(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")

include(CheckCXXCompilerFlag)

CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
CHECK_CXX_COMPILER_FLAG("-std=c++0x" COMPILER_SUPPORTS_CXX0X)
SET(CXX_STD_FLAG "")
if(COMPILER_SUPPORTS_CXX11)
        SET(CXX_STD_FLAG "-std=c++11")
elseif(COMPILER_SUPPORTS_CXX0X)
        SET(CXX_STD_FLAG "-std=c++0x")
else()
  message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support. Please use a different C++ compiler.")
endif()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_STD_FLAG}")

FIND_PACKAGE(Sqlite REQUIRED)
SET(PostgreSQL_ADDITIONAL_SEARCH_PATHS ${PostgreSQL_ADDITIONAL_SEARCH_PATHS} "/usr/include/pgsql/")
FIND_PACKAGE(PostgreSQL REQUIRED)

find_package(OptionParser REQUIRED COMPONENT MAYBEBUILTIN)
include_directories(${OptionParser_INCLUDE_DIRS})
SET(CMAKE_CXX_FLAGS "${OptionParser_CFLAGS} ${CMAKE_CXX_FLAGS}")

# Finally, the main compilation...
ADD_SUBDIRECTORY(src)
