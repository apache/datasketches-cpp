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
#include <var_opt_sketch.hpp>

namespace datasketches {

// assume the binary sketches for this test have been generated by datasketches-java code
// in the subdirectory called "java" in the root directory of this project
static std::string testBinaryInputPath = std::string(TEST_BINARY_INPUT_PATH) + "../../java/";

TEST_CASE("var opt sketch long", "[serde_compat]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "varopt_sketch_long_n" + std::to_string(n) + "_java.sk", std::ios::binary);
    const auto sketch = var_opt_sketch<long>::deserialize(is);
    REQUIRE(sketch.is_empty() == (n == 0));
    REQUIRE(sketch.get_num_samples() == (n > 10 ? 32 : n));
  }
}

TEST_CASE("var opt sketch: deserialize exact from java", "[serde_compat]") {
  const double EPS = 1e-13;
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "varopt_sketch_string_exact_java.sk", std::ios::binary);
  const auto sketch = var_opt_sketch<std::string>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_k() == 1024);
  REQUIRE(sketch.get_n() == 200);
  REQUIRE(sketch.get_num_samples() == 200);
  const subset_summary ss = sketch.estimate_subset_sum([](std::string){ return true; });

  double tgt_wt = 0.0;
  for (int i = 1; i <= 200; ++i) { tgt_wt += 1000.0 / i; }
  REQUIRE(ss.total_sketch_weight == Approx(tgt_wt).margin(EPS));
}


TEST_CASE("var opt sketch: deserialize sampling from java", "[serde_compat]") {
  const double EPS = 1e-13;
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(testBinaryInputPath + "varopt_sketch_long_sampling_java.sk", std::ios::binary);
  const auto sketch = var_opt_sketch<int64_t>::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_k() == 1024);
  REQUIRE(sketch.get_n() == 2003);
  REQUIRE(sketch.get_num_samples() == sketch.get_k());
  subset_summary ss = sketch.estimate_subset_sum([](int64_t){ return true; });
  REQUIRE(ss.estimate == Approx(332000.0).margin(EPS));
  REQUIRE(ss.total_sketch_weight == Approx(332000.0).margin(EPS));

  ss = sketch.estimate_subset_sum([](int64_t x){ return x < 0; });
  REQUIRE(ss.estimate == 330000.0); // heavy item, weight is exact

  ss = sketch.estimate_subset_sum([](int64_t x){ return x >= 0; });
  REQUIRE(ss.estimate == Approx(2000.0).margin(EPS));
}

} /* namespace datasketches */
