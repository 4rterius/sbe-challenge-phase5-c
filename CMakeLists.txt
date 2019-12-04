cmake_minimum_required(VERSION 3.10)
project(sbe_challenge_phase5 C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Wall -O0 -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Wall -O3 -s")

include(FindPkgConfig)
pkg_check_modules(LIBMYSQLCLIENT REQUIRED mysqlclient)
foreach(FLAG ${LIBMYSQLCLIENT_CFLAGS_OTHER})
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${FLAG}")
endforeach()
link_directories(${LIBMYSQLCLIENT_LIBRARY_DIRS})

add_executable(my_import_quantities src/main.c)
target_include_directories(my_import_quantities PRIVATE ${LIBMYSQLCLIENT_INCLUDE_DIRS})
target_link_libraries(my_import_quantities PRIVATE ${LIBMYSQLCLIENT_LIBRARIES})