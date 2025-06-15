# Install script for directory: /home/lucky/db2025/rmdb/src

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/usr/local")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "Debug")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set path to fallback-tool for dependency-resolution.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for each subdirectory.
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/analyze/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/record/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/index/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/system/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/execution/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/parser/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/optimizer/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/storage/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/common/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/replacer/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/transaction/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/recovery/cmake_install.cmake")
  include("/home/lucky/db2025/rmdb/src/cmake-build-debug/test/cmake_install.cmake")

endif()

if(CMAKE_INSTALL_COMPONENT)
  if(CMAKE_INSTALL_COMPONENT MATCHES "^[a-zA-Z0-9_.+-]+$")
    set(CMAKE_INSTALL_MANIFEST "install_manifest_${CMAKE_INSTALL_COMPONENT}.txt")
  else()
    string(MD5 CMAKE_INST_COMP_HASH "${CMAKE_INSTALL_COMPONENT}")
    set(CMAKE_INSTALL_MANIFEST "install_manifest_${CMAKE_INST_COMP_HASH}.txt")
    unset(CMAKE_INST_COMP_HASH)
  endif()
else()
  set(CMAKE_INSTALL_MANIFEST "install_manifest.txt")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  string(REPLACE ";" "\n" CMAKE_INSTALL_MANIFEST_CONTENT
       "${CMAKE_INSTALL_MANIFEST_FILES}")
  file(WRITE "/home/lucky/db2025/rmdb/src/cmake-build-debug/${CMAKE_INSTALL_MANIFEST}"
     "${CMAKE_INSTALL_MANIFEST_CONTENT}")
endif()
