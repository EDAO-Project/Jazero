cmake_minimum_required(VERSION 3.16)
project(jdlc C)

include(CTest)

set(CMAKE_C_STANDARD 11)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib/)

add_compile_options(-Wall -Wextra -pedantic -Werror)

include_directories(include/)
add_library(jdlc
            src/jdlc.c
            src/structures/property.c
            src/connection/address.c src/params.c)

add_executable(test_address test/connection/address_test.c)
add_executable(test_property test/structures/property_test.c)
target_link_libraries(test_address PRIVATE jdlc)
target_link_libraries(test_property PRIVATE jdlc)
enable_testing()

add_test(NAME test_address COMMAND $<TARGET_FILE:test_address>)
add_test(NAME test_property COMMAND $<TARGET_FILE:test_property>)