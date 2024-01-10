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
#include <iostream>

#include "tdigest.hpp"

namespace datasketches {

TEST_CASE("empty", "[tdigest]") {
  tdigest_double td(10);
//  std::cout << td.to_string();
  REQUIRE(td.is_empty());
  REQUIRE(td.get_k() == 10);
  REQUIRE(td.get_total_weight() == 0);
  REQUIRE_THROWS_AS(td.get_min_value(), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_max_value(), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_rank(0), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_quantile(0.5), std::runtime_error);
}

TEST_CASE("one value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  REQUIRE(td.get_k() == 100);
  REQUIRE(td.get_total_weight() == 1);
  REQUIRE(td.get_min_value() == 1);
  REQUIRE(td.get_max_value() == 1);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.5);
  REQUIRE(td.get_rank(1.01) == 1);
}

TEST_CASE("many values", "[tdigest]") {
  const size_t n = 10000;
  tdigest_double td(100);
  for (size_t i = 0; i < n; ++i) td.update(i);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE_FALSE(td.is_empty());
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

TEST_CASE("rank - two values", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(2);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.25);
  REQUIRE(td.get_rank(1.25) == 0.375);
  REQUIRE(td.get_rank(1.5) == 0.5);
  REQUIRE(td.get_rank(1.75) == 0.625);
  REQUIRE(td.get_rank(2) == 0.75);
  REQUIRE(td.get_rank(2.01) == 1);
}

TEST_CASE("rank - repeated value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(1);
  td.update(1);
  td.update(1);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.5);
  REQUIRE(td.get_rank(1.01) == 1);
}

TEST_CASE("rank - repeated block", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(2);
  td.update(2);
  td.update(3);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.125);
  REQUIRE(td.get_rank(2) == 0.5);
  REQUIRE(td.get_rank(3) == 0.875);
  REQUIRE(td.get_rank(3.01) == 1);
}

TEST_CASE("merge small", "[tdigest]") {
  tdigest_double td1(10);
  td1.update(1);
  td1.update(2);
  tdigest_double td2(10);
  td2.update(2);
  td2.update(3);
  td1.merge(td2);
  REQUIRE(td1.get_min_value() == 1);
  REQUIRE(td1.get_max_value() == 3);
  REQUIRE(td1.get_total_weight() == 4);
  REQUIRE(td1.get_rank(0.99) == 0);
  REQUIRE(td1.get_rank(1) == 0.125);
  REQUIRE(td1.get_rank(2) == 0.5);
  REQUIRE(td1.get_rank(3) == 0.875);
  REQUIRE(td1.get_rank(3.01) == 1);
}

TEST_CASE("merge large", "[tdigest]") {
  const size_t n = 10000;
  tdigest_double td1(100);
  tdigest_double td2(100);
  for (size_t i = 0; i < n / 2; ++i) {
    td1.update(i);
    td2.update(n / 2 + i);
  }
  td1.merge(td2);
//  td1.compress();
//  std::cout << td1.to_string(true);
  REQUIRE(td1.get_total_weight() == n);
  REQUIRE(td1.get_min_value() == 0);
  REQUIRE(td1.get_max_value() == n - 1);
  REQUIRE(td1.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td1.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td1.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td1.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td1.get_rank(n) == 1);
}

} /* namespace datasketches */
