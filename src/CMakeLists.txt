cmake_minimum_required(VERSION 3.13)
project(rmdb)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

# 设置 CMake 政策
cmake_policy(SET CMP0079 NEW)

# 添加子目录
add_subdirectory(analyze)
add_subdirectory(record)
add_subdirectory(index)
add_subdirectory(system)
add_subdirectory(execution)
add_subdirectory(parser)
add_subdirectory(optimizer)
add_subdirectory(storage)
add_subdirectory(common)
add_subdirectory(replacer)
add_subdirectory(transaction)
add_subdirectory(recovery)
add_subdirectory(test)

# 链接库
target_link_libraries(parser execution pthread)

# rmdb 可执行文件
add_executable(rmdb rmdb.cpp)
target_link_libraries(rmdb parser execution readline pthread planner analyze)

# unit_test 可执行文件
add_executable(unit_test unit_test.cpp)
target_link_libraries(unit_test storage lru_replacer record gtest_main)