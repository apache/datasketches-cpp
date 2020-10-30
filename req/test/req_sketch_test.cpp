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

#include <catch.hpp>

#include <req_sketch.hpp>

#include <limits>

namespace datasketches {

#ifdef TEST_BINARY_INPUT_PATH
const std::string inputPath = TEST_BINARY_INPUT_PATH;
#else
const std::string inputPath = "test/";
#endif

TEST_CASE("req sketch: empty", "[req_sketch]") {
  req_sketch<float, true> sketch(100);
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 0);
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(std::isnan(sketch.get_rank(0)));
  REQUIRE(std::isnan(sketch.get_rank(std::numeric_limits<float>::infinity())));
}

TEST_CASE("req sketch: single value", "[req_sketch]") {
  req_sketch<float, true> sketch(100);
  sketch.update(1);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 1);
  REQUIRE(sketch.get_num_retained() == 1);
  REQUIRE(sketch.get_rank(1) == 0);
  REQUIRE(sketch.get_rank<true>(1) == 1);
  REQUIRE(sketch.get_rank(1.1) == 1);
  REQUIRE(sketch.get_rank(std::numeric_limits<float>::infinity()) == 1);
}

TEST_CASE("req sketch: repeated values", "[req_sketch]") {
  req_sketch<float, true> sketch(100);
  sketch.update(1);
  sketch.update(1);
  sketch.update(1);
  sketch.update(2);
  sketch.update(2);
  sketch.update(2);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 6);
  REQUIRE(sketch.get_num_retained() == 6);
  REQUIRE(sketch.get_rank(1) == 0);
  REQUIRE(sketch.get_rank<true>(1) == 0.5);
  REQUIRE(sketch.get_rank(2) == 0.5);
  REQUIRE(sketch.get_rank<true>(2) == 1);
}

TEST_CASE("req sketch: exact mode", "[req_sketch]") {
  req_sketch<float, true> sketch(100);
  for (size_t i = 0; i < 100; ++i) sketch.update(i);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == 100);
  REQUIRE(sketch.get_num_retained() == 100);
  REQUIRE(sketch.get_rank(50) == 0.5);
  REQUIRE(sketch.get_rank<true>(49) == 0.5);
}

TEST_CASE("req sketch: estimation mode", "[req_sketch]") {
  std::cout << "estimation mode test\n";
  req_sketch<float, true> sketch(100);
  const size_t n = 1250;
  for (size_t i = 0; i < n; ++i) sketch.update(i);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_n() == n);
  std::cout << sketch.to_string(true, true);
  REQUIRE(sketch.get_num_retained() < n);
  REQUIRE(sketch.get_rank(0) == 0);
  REQUIRE(sketch.get_rank(n) == 1);
  REQUIRE(sketch.get_rank(n / 2) == Approx(0.5).margin(0.01));
  REQUIRE(sketch.get_rank(n - 1) == Approx(1).margin(0.01));
}

} /* namespace datasketches */
