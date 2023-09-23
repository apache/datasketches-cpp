/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <ebpps_sample.hpp>

#include <catch2/catch.hpp>

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <cmath>
#include <random>
#include <stdexcept>

namespace datasketches {

static constexpr double EPS = 1e-13;


TEST_CASE("ebpps sample: basic initialization", "[ebpps_sketch]") {
  ebpps_sample<int> sample = ebpps_sample<int>(0);
  REQUIRE(sample.get_c() == 0.0);
  REQUIRE(sample.get_num_retained_items() == 0);
  REQUIRE(sample.get_sample().size() == 0);
}

TEST_CASE("ebpps sample: pre-initialized", "[ebpps_sketch]") {
  double theta = 1.0;
  ebpps_sample<int> sample = ebpps_sample<int>(-1, theta);
  REQUIRE(sample.get_c() == theta);
  REQUIRE(sample.get_num_retained_items() == 1);
  REQUIRE(sample.get_sample().size() == 1);

  theta = 1e-300;
  sample = ebpps_sample<int>(-1, theta);
  REQUIRE(sample.get_c() == theta);
  REQUIRE(sample.get_num_retained_items() == 1);
  REQUIRE(sample.get_sample().size() == 0); // assuming the random number is > 1e-300
}

TEST_CASE("ebpps sample: merge unit samples", "[ebpps_sketch]") {
  uint32_t k = 8;
  ebpps_sample<int> sample = ebpps_sample<int>(k);
  
  for (uint32_t i = 1; i <= k; ++i) {
    ebpps_sample<int> s = ebpps_sample<int>(i, 1.0);
    sample.merge(s);
    REQUIRE(sample.get_c() == static_cast<double>(i));
    REQUIRE(sample.get_num_retained_items() == i);
  }
}

} // namespace datasketches
