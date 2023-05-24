cmake_minimum_required(VERSION 3.16)
project(jdlc C)

include(CTest)
include(ExternalProject)
include(FetchContent)
find_package(Git REQUIRED)

set(CMAKE_C_COMPILER "gcc")
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_COMPILER_ID "GNU")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib/)

add_compile_options(-Wall -Wextra -pedantic -Werror)

if(WIN32)
    add_compile_definitions(WINDOWS)
endif()

if(LINUX OR UNIX)
    add_compile_definitions(UNIX)
endif()

set(CJSON_ROOT ${CMAKE_BINARY_DIR}/cJSON)

if(NOT EXISTS ${CJSON_ROOT})
    execute_process(COMMAND git clone https://github.com/DaveGamble/cJSON.git)
endif()

include_directories(include/)
include_directories(${CJSON_ROOT})
file(GLOB jdlc_src
        src/connection/*.c
        src/driver/*.c
        src/structures/*.c
        src/utils/*.c
        src/jazero.c)
file(GLOB cjson_src ${CJSON_ROOT}/*.c)
add_library(jdlc-core STATIC ${jdlc_src})
add_library(cjson-core STATIC ${cjson_src})
target_link_libraries(jdlc-core cjson-core -lcurl)

add_executable(jdlc_cmd src/cmd.c)
target_link_libraries(jdlc_cmd jdlc-core)

add_executable(test_address test/connection/address_test.c)
add_executable(test_request test/connection/request_test.c)
add_executable(test_property test/structures/property_test.c)
add_executable(test_params test/params_test.c)
add_executable(test_json_body test/json_body_test.c)
add_executable(test_query test/structures/query_test.c)
add_executable(test_file_utils test/utils/file_utils_test.c)
target_link_libraries(test_address PRIVATE jdlc-core)
target_link_libraries(test_request PRIVATE jdlc-core)
target_link_libraries(test_property PRIVATE jdlc-core)
target_link_libraries(test_params PRIVATE jdlc-core)
target_link_libraries(test_json_body PRIVATE jdlc-core)
target_link_libraries(test_query PRIVATE jdlc-core)
target_link_libraries(test_file_utils PRIVATE jdlc-core)
enable_testing()

add_test(NAME test_address COMMAND $<TARGET_FILE:test_address>)
add_test(NAME test_property COMMAND $<TARGET_FILE:test_property>)
add_test(NAME test_request COMMAND $<TARGET_FILE:test_request>)
add_test(NAME test_params COMMAND $<TARGET_FILE:test_params>)
add_test(NAME test_json_body COMMAND $<TARGET_FILE:test_json_body>)
add_test(NAME test_query COMMAND $<TARGET_FILE:test_query>)
add_test(NAME test_file_utils COMMAND $<TARGET_FILE:test_file_utils>)
