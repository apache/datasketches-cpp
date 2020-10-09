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

add_library(tuple INTERFACE)

add_library(${PROJECT_NAME}::TUPLE ALIAS tuple)

if (BUILD_TESTS)
  add_subdirectory(test)
endif()

target_include_directories(tuple
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:$<INSTALL_PREFIX>/include>
)

target_link_libraries(tuple INTERFACE common)
target_compile_features(tuple INTERFACE cxx_std_11)

set(tuple_HEADERS "")
list(APPEND tuple_HEADERS "include/tuple_sketch.hpp;include/tuple_sketch_impl.hpp")
list(APPEND tuple_HEADERS "include/tuple_union.hpp;include/tuple_union_impl.hpp")
list(APPEND tuple_HEADERS "include/tuple_intersection.hpp;include/tuple_intersection_impl.hpp")
list(APPEND tuple_HEADERS "include/tuple_a_not_b.hpp;include/tuple_a_not_b_impl.hpp")
list(APPEND tuple_HEADERS "include/array_of_doubles_sketch.hpp;include/array_of_doubles_sketch_impl.hpp")
list(APPEND tuple_HEADERS "include/array_of_doubles_union.hpp;include/array_of_doubles_union_impl.hpp")
list(APPEND tuple_HEADERS "include/array_of_doubles_intersection.hpp;include/array_of_doubles_intersection_impl.hpp")
list(APPEND tuple_HEADERS "include/array_of_doubles_a_not_b.hpp;include/array_of_doubles_a_not_b_impl.hpp")
list(APPEND tuple_HEADERS "include/theta_update_sketch_base.hpp;include/theta_update_sketch_base_impl.hpp")
list(APPEND tuple_HEADERS "include/theta_union_base.hpp;include/theta_union_base_impl.hpp")
list(APPEND tuple_HEADERS "include/theta_intersection_base.hpp;include/theta_intersection_base_impl.hpp")
list(APPEND tuple_HEADERS "include/theta_set_difference_base.hpp;include/theta_set_difference_base_impl.hpp")
list(APPEND tuple_HEADERS "include/theta_sketch_experimental.hpp;include/theta_sketch_experimental_impl.hpp")
list(APPEND tuple_HEADERS "include/theta_union_experimental.hpp;include/theta_union_experimental_impl.hpp")
list(APPEND tuple_HEADERS "include/theta_intersection_experimental.hpp;include/theta_intersection_experimental_impl.hpp")
list(APPEND tuple_HEADERS "include/theta_a_not_b_experimental.hpp;include/theta_a_not_b_experimental_impl.hpp")
list(APPEND tuple_HEADERS "include/bounds_on_ratios_in_sampled_sets.hpp")
list(APPEND tuple_HEADERS "include/bounds_on_ratios_in_theta_sketched_sets.hpp")
list(APPEND tuple_HEADERS "include/jaccard_similarity.hpp")
list(APPEND tuple_HEADERS "include/theta_comparators.hpp")
list(APPEND tuple_HEADERS "include/theta_cnstants.hpp")

install(TARGETS tuple
  EXPORT ${PROJECT_NAME}
)

install(FILES ${tuple_HEADERS}
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/DataSketches")

target_sources(tuple
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include/tuple_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/tuple_sketch_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/tuple_union.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/tuple_union_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/tuple_intersection.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/tuple_intersection_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/tuple_a_not_b.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/tuple_a_not_b_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/array_of_doubles_sketch.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/array_of_doubles_sketch_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/array_of_doubles_union.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/array_of_doubles_union_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/array_of_doubles_intersection.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/array_of_doubles_intersection_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/array_of_doubles_a_not_b.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/array_of_doubles_a_not_b_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_update_sketch_base.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_update_sketch_base_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_union_base.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_union_base_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_intersection_base.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_intersection_base_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_set_difference_base.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_set_difference_base_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_sketch_experimental.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_sketch_experimental_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_union_experimental.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_union_experimental_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_intersection_experimental.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_intersection_experimental_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_a_not_b_experimental.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_a_not_b_experimental_impl.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/bounds_on_ratios_in_sampled_sets.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/bounds_on_ratios_in_theta_sketched_sets.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/jaccard_similarity.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_comparators.hpp
    ${CMAKE_CURRENT_SOURCE_DIR}/include/theta_constants.hpp
)
