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

#include <catch2/catch.hpp>
#include <fstream>
#include <theta_sketch.hpp>

namespace datasketches {

// assume the binary sketches for this test have been generated by datasketches-java code
// in the subdirectory called "java" in the root directory of this project
static std::string testBinaryInputPath = std::string(TEST_BINARY_INPUT_PATH) + "../../java/";

TEST_CASE("theta sketch", "[serde_compat]") {
  unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (unsigned n: n_arr) {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "theta_n" + std::to_string(n) + ".sk", std::ios::binary);
    auto sketch = compact_theta_sketch::deserialize(is);
    REQUIRE(sketch.is_empty() == (n == 0));
    REQUIRE(sketch.is_estimation_mode() == (n > 1000));
    REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.03));
    for (auto hash: sketch) {
      REQUIRE(hash < sketch.get_theta64());
    }
    REQUIRE(sketch.is_ordered());
    REQUIRE(std::is_sorted(sketch.begin(), sketch.end()));
  }
}

TEST_CASE("theta sketch non-empty no entries", "[serde_compat]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "theta_non_empty_no_entries.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_num_retained() == 0);
}

} /* namespace datasketches */
