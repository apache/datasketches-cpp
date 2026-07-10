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

if (NOT TARGET benchmark::benchmark_main)
  include(FetchContent)

  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Disable google-benchmark tests" FORCE)
  set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "Disable google-benchmark gtest tests" FORCE)
  set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "Disable google-benchmark install rules" FORCE)
  set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "Disable google-benchmark documentation install" FORCE)
  set(BENCHMARK_ENABLE_WERROR OFF CACHE BOOL "Disable google-benchmark warnings as errors" FORCE)

  FetchContent_Declare(
    googlebenchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG        v1.7.1
  )

  FetchContent_MakeAvailable(googlebenchmark)
endif()

add_executable(count_min_sketch_benchmark)

target_link_libraries(count_min_sketch_benchmark
  PRIVATE
    count
    benchmark::benchmark_main
)

set_target_properties(count_min_sketch_benchmark PROPERTIES
  CXX_STANDARD_REQUIRED YES
)

set(COUNT_MIN_SKETCH_BENCHMARK_SOURCES
  benchmark_count_min_sketch_serialization.cpp
)

if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/benchmark_count_min_sketch.cpp")
  list(APPEND COUNT_MIN_SKETCH_BENCHMARK_SOURCES benchmark_count_min_sketch.cpp)
endif()

target_sources(count_min_sketch_benchmark
  PRIVATE
    ${COUNT_MIN_SKETCH_BENCHMARK_SOURCES}
)
