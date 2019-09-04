# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

add_library(cpc OBJECT "")

add_library(${PROJECT_NAME}::CPC ALIAS cpc)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(cpc
  PUBLIC
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  PRIVATE 
    ${COMMON_INCLUDE_DIR}
)

target_link_libraries(cpc common)

set_target_properties(cpc PROPERTIES
  PUBLIC_HEADER "include/cpc_sketch.hpp;include/cpc_union.hpp;include/cpc_common.hpp"
  POSITION_INDEPENDENT_CODE ON
  CXX_STANDARD 11
  CXX_STANDARD_REQUIRED YES
)

install(TARGETS cpc
  EXPORT ${PROJECT_NAME}
  PUBLIC_HEADER DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/DataSketches
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/DataSketches
)

target_sources(cpc
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_union.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cpc_common.hpp
  PRIVATE
    src/compressionData.data
    src/cpc_sketch.cpp
    src/fm85.cpp
    src/fm85Compression.cpp
    src/fm85Confidence.cpp
    src/fm85Merging.cpp
    src/fm85Util.cpp
    src/iconEstimator.cpp
    src/u32Table.cpp
    include/common.h
    include/fm85.h
    include/fm85Compression.h
    include/fm85Confidence.h
    include/fm85Merging.h
    include/fm85Util.h
    include/iconEstimator.h
    include/u32Table.h
)
