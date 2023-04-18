cmake_minimum_required(VERSION 3.16)
project(jdlc C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib/)

add_compile_options(-Wall -Wextra -pedantic -Werror)

include_directories(include/)
add_library(jdlc
        src/jdlc.c
        include/jdlc.h)