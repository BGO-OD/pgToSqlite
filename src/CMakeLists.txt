include_directories(${SQLITE_INCLUDE_DIRS} ${PostgreSQL_INCLUDE_DIRS})

add_executable(pgToSqlite pgToSqlite.cpp)
target_link_libraries(pgToSqlite ${OptionParser_LIBRARIES} ${SQLITE_LIBRARIES} ${PostgreSQL_LIBRARIES})
install(TARGETS pgToSqlite DESTINATION bin)
