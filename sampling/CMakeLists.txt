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

add_library(sampling INTERFACE)

add_library(${PROJECT_NAME}::SAMPLING ALIAS sampling)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(sampling
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
)

target_link_libraries(sampling INTERFACE common)
target_compile_features(sampling INTERFACE cxx_std_11)

set(sampling_HEADERS "include/var_opt_sketch.hpp;include/var_opt_sketch_impl.hpp")

install(TARGETS sampling
  EXPORT ${PROJECT_NAME}
)

install(FILES ${sampling_HEADERS}
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")

target_sources(sampling
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/var_opt_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/var_opt_sketch_impl.hpp
)
