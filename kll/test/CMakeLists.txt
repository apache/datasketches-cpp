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

add_executable(kll_test)

target_link_libraries(kll_test kll common_test_lib)

set_target_properties(kll_test PROPERTIES
  CXX_STANDARD_REQUIRED YES
)

file(TO_CMAKE_PATH "${CMAKE_CURRENT_SOURCE_DIR}" KLL_TEST_BINARY_PATH)
string(APPEND KLL_TEST_BINARY_PATH "/")
target_compile_definitions(kll_test
  PRIVATE
    TEST_BINARY_INPUT_PATH="${KLL_TEST_BINARY_PATH}"
)

add_test(
  NAME kll_test
  COMMAND kll_test
)

target_sources(kll_test
  PRIVATE
    kll_sketch_test.cpp
    kll_sketch_custom_type_test.cpp
    kll_sketch_validation.cpp
    kolmogorov_smirnov_test.cpp
)

if (SERDE_COMPAT)
target_sources(kll_test
  PRIVATE
    kll_sketch_deserialize_from_java_test.cpp
)
endif()

if (GENERATE)
target_sources(kll_test
  PRIVATE
    kll_sketch_serialize_for_java.cpp
)
endif()
