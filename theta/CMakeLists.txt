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

add_library(theta INTERFACE)

add_library(${PROJECT_NAME}::THETA ALIAS theta)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(theta
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
)

target_link_libraries(theta INTERFACE common)
target_compile_features(theta INTERFACE cxx_std_11)

set(theta_HEADERS "")
list(APPEND theta_HEADERS "include/theta_sketch.hpp;include/theta_union.hpp;include/theta_intersection.hpp")
list(APPEND theta_HEADERS "include/theta_a_not_b.hpp;include/binomial_bounds.hpp;include/theta_sketch_impl.hpp")
list(APPEND theta_HEADERS "include/theta_union_impl.hpp;include/theta_intersection_impl.hpp;include/theta_a_not_b_impl.hpp")
list(APPEND theta_HEADERS "include/conditional_back_inserter.hpp")

install(TARGETS theta
  EXPORT ${PROJECT_NAME}
)

install(FILES ${theta_HEADERS}
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")

target_sources(theta
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_union.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_intersection.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_a_not_b.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/binomial_bounds.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_sketch_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_union_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_intersection_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_a_not_b_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/conditional_back_inserter.hpp
)
