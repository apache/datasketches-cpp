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

# separate executables for var_opt and ebpps sampling

# BLOOM FILTER
add_executable(bloom_filter_test)

target_link_libraries(bloom_filter_test filters common_test_lib)

set_target_properties(bloom_filter_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
)

file(TO_CMAKE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" FILTERS_TEST_BINARY_PATH)
string(APPEND FILTERS_TEST_BINARY_PATH "/")
target_compile_definitions(bloom_filter_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${FILTERS_TEST_BINARY_PATH}"
)

add_test(
  NAME bloom_filter_test
  COMMAND bloom_filter_test
)

target_sources(bloom_filter_test
  PRIVATE
  bit_array_test.cpp
  bloom_filter_test.cpp
)
