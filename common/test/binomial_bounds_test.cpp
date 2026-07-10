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

#include "binomial_bounds.hpp"

namespace datasketches {

TEST_CASE("binomial_bounds: get_lower_bound", "[common]") {

  SECTION("num_samples == 0") {
    double result = binomial_bounds::get_lower_bound(0, 0.5, 1);
    REQUIRE(result == 0.0);
  }

  SECTION("theta == 1.0") {
    double result = binomial_bounds::get_lower_bound(100, 1.0, 1);
    REQUIRE(result == 100.0);
  }

  SECTION("num_samples == 1") {
    double result = binomial_bounds::get_lower_bound(1, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples == 1, stddev=2") {
    double result = binomial_bounds::get_lower_bound(1, 0.5, 2);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples == 1, stddev=3") {
    double result = binomial_bounds::get_lower_bound(1, 0.5, 3);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples > 120") {
    double result = binomial_bounds::get_lower_bound(121, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples > 120, stddev=2") {
    double result = binomial_bounds::get_lower_bound(200, 0.5, 2);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples > 120, stddev=3") {
    double result = binomial_bounds::get_lower_bound(500, 0.5, 3);
    REQUIRE(result >= 0.0);
  }

  SECTION("2 <= num_samples <= 120 AND theta > (1-1e-5)") {
    double result = binomial_bounds::get_lower_bound(50, 1.0 - 1e-6, 1);
    REQUIRE(std::abs(result - 50.0) < 50.0 * 0.01);
  }

  SECTION("2 <= num_samples <= 120 AND theta > (1-1e-5), stddev=2") {
    double result = binomial_bounds::get_lower_bound(50, 1.0 - 1e-6, 2);
    REQUIRE(std::abs(result - 50.0) < 50.0 * 0.01);
  }

  SECTION("2 <= num_samples <= 120 AND theta > (1-1e-5), stddev=3") {
    double result = binomial_bounds::get_lower_bound(50, 1.0 - 1e-6, 3);
    REQUIRE(std::abs(result - 50.0) < 50.0 * 0.01);
  }

  SECTION("2 <= num_samples <= 120 AND theta < num_samples/360") {
    double result = binomial_bounds::get_lower_bound(100, 0.001, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("2 <= num_samples <= 120 AND theta < num_samples/360, stddev=2") {
    double result = binomial_bounds::get_lower_bound(100, 0.001, 2);
    REQUIRE(result >= 0.0);
  }

  SECTION("2 <= num_samples <= 120 AND theta < num_samples/360, stddev=3") {
    double result = binomial_bounds::get_lower_bound(100, 0.001, 3);
    REQUIRE(result >= 0.0);
  }

  SECTION("2 <= num_samples <= 120 AND middle range theta (exact calculation)") {
    double result = binomial_bounds::get_lower_bound(10, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("2 <= num_samples <= 120 AND middle range theta, stddev=2") {
    double result = binomial_bounds::get_lower_bound(10, 0.5, 2);
    REQUIRE(result >= 0.0);
  }

  SECTION("2 <= num_samples <= 120 AND middle range theta, stddev=3") {
    double result = binomial_bounds::get_lower_bound(10, 0.5, 3);
    REQUIRE(result >= 0.0);
  }

  SECTION("theta=0") {
    REQUIRE_THROWS_AS(binomial_bounds::get_lower_bound(10, 0.0, 1), std::invalid_argument);
  }

  SECTION("theta very close to 0") {
    double result = binomial_bounds::get_lower_bound(10, 1e-10, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples=2 boundary") {
    double result = binomial_bounds::get_lower_bound(2, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples=120 boundary") {
    double result = binomial_bounds::get_lower_bound(120, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("estimate clamping case") {
    double result = binomial_bounds::get_lower_bound(10, 0.9, 1);
    double estimate = 10.0 / 0.9;
    REQUIRE(result <= estimate);
  }

  SECTION("invalid theta < 0") {
    REQUIRE_THROWS_AS(binomial_bounds::get_lower_bound(100, -0.1, 1), std::invalid_argument);
  }

  SECTION("invalid theta > 1") {
    REQUIRE_THROWS_AS(binomial_bounds::get_lower_bound(100, 1.1, 1), std::invalid_argument);
  }

  SECTION("invalid stddev = 0") {
    REQUIRE_THROWS_AS(binomial_bounds::get_lower_bound(100, 0.5, 0), std::invalid_argument);
  }

  SECTION("invalid stddev = 4") {
    REQUIRE_THROWS_AS(binomial_bounds::get_lower_bound(100, 0.5, 4), std::invalid_argument);
  }
}

TEST_CASE("binomial_bounds: get_upper_bound", "[common]") {

  SECTION("theta == 1.0") {
    double result = binomial_bounds::get_upper_bound(100, 1.0, 1);
    REQUIRE(result == 100.0);
  }

  SECTION("num_samples == 0") {
    double result = binomial_bounds::get_upper_bound(0, 0.5, 1);
    REQUIRE(result > 0.0);
  }

  SECTION("num_samples == 0, stddev=2") {
    double result = binomial_bounds::get_upper_bound(0, 0.5, 2);
    REQUIRE(result > 0.0);
  }

  SECTION("num_samples == 0, stddev=3") {
    double result = binomial_bounds::get_upper_bound(0, 0.5, 3);
    REQUIRE(result > 0.0);
  }

  SECTION("num_samples > 120") {
    double result = binomial_bounds::get_upper_bound(121, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples > 120, stddev=2") {
    double result = binomial_bounds::get_upper_bound(200, 0.5, 2);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples > 120, stddev=3") {
    double result = binomial_bounds::get_upper_bound(500, 0.5, 3);
    REQUIRE(result >= 0.0);
  }

  SECTION("1 <= num_samples <= 120 AND theta > (1-1e-5)") {
    double result = binomial_bounds::get_upper_bound(50, 1.0 - 1e-6, 1);
    REQUIRE(result == 51.0);
  }

  SECTION("1 <= num_samples <= 120 AND theta > (1-1e-5), stddev=2") {
    double result = binomial_bounds::get_upper_bound(50, 1.0 - 1e-6, 2);
    REQUIRE(result == 51.0);
  }

  SECTION("1 <= num_samples <= 120 AND theta > (1-1e-5), stddev=3") {
    double result = binomial_bounds::get_upper_bound(50, 1.0 - 1e-6, 3);
    REQUIRE(result == 51.0);
  }

  SECTION("1 <= num_samples <= 120 AND theta < num_samples/360") {
    double result = binomial_bounds::get_upper_bound(100, 0.001, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("1 <= num_samples <= 120 AND theta < num_samples/360, stddev=2") {
    double result = binomial_bounds::get_upper_bound(100, 0.001, 2);
    REQUIRE(result >= 0.0);
  }

  SECTION("1 <= num_samples <= 120 AND theta < num_samples/360, stddev=3") {
    double result = binomial_bounds::get_upper_bound(100, 0.001, 3);
    REQUIRE(result >= 0.0);
  }

  SECTION("1 <= num_samples <= 120 AND middle range theta (exact calculation)") {
    double result = binomial_bounds::get_upper_bound(10, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("1 <= num_samples <= 120 AND middle range theta, stddev=2") {
    double result = binomial_bounds::get_upper_bound(10, 0.5, 2);
    REQUIRE(result >= 0.0);
  }

  SECTION("1 <= num_samples <= 120 AND middle range theta, stddev=3") {
    double result = binomial_bounds::get_upper_bound(10, 0.5, 3);
    REQUIRE(result >= 0.0);
  }

  SECTION("theta=0") {
    REQUIRE_THROWS_AS(binomial_bounds::get_upper_bound(10, 0.0, 1), std::invalid_argument);
  }

  SECTION("theta very close to 0") {
    double result = binomial_bounds::get_upper_bound(10, 1e-10, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples=1 boundary") {
    double result = binomial_bounds::get_upper_bound(1, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("num_samples=120 boundary") {
    double result = binomial_bounds::get_upper_bound(120, 0.5, 1);
    REQUIRE(result >= 0.0);
  }

  SECTION("estimate clamping case") {
    double result = binomial_bounds::get_upper_bound(10, 0.9, 1);
    double estimate = 10.0 / 0.9;
    REQUIRE(result >= estimate);
  }

  SECTION("invalid theta < 0") {
    REQUIRE_THROWS_AS(binomial_bounds::get_upper_bound(100, -0.1, 1), std::invalid_argument);
  }

  SECTION("invalid theta > 1") {
    REQUIRE_THROWS_AS(binomial_bounds::get_upper_bound(100, 1.1, 1), std::invalid_argument);
  }

  SECTION("invalid stddev = 0") {
    REQUIRE_THROWS_AS(binomial_bounds::get_upper_bound(100, 0.5, 0), std::invalid_argument);
  }

  SECTION("invalid stddev = 4") {
    REQUIRE_THROWS_AS(binomial_bounds::get_upper_bound(100, 0.5, 4), std::invalid_argument);
  }
}

} /* namespace datasketches */
