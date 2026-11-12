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

#include "cpc_union.hpp"

#include <stdexcept>

namespace datasketches {

static const double RELATIVE_ERROR_FOR_LG_K_11 = 0.02;

TEST_CASE("cpc union: lg k limits", "[cpc_union]") {
  cpc_union u1(cpc_constants::MIN_LG_K); // this should work
  cpc_union u2(cpc_constants::MAX_LG_K); // this should work
  REQUIRE_THROWS_AS(cpc_union(cpc_constants::MIN_LG_K - 1), std::invalid_argument);
  REQUIRE_THROWS_AS(cpc_union(cpc_constants::MAX_LG_K + 1), std::invalid_argument);
}

TEST_CASE("cpc union: empty", "[cpc_union]") {
  cpc_union u(11);
  auto s = u.get_result();
  REQUIRE(s.is_empty());
  REQUIRE(s.get_estimate() == 0.0);
}

TEST_CASE("cpc union: copy", "[cpc_union]") {
  cpc_sketch s(11);
  s.update(1);
  cpc_union u1(11);
  u1.update(s);

  cpc_union u2 = u1; // copy constructor
  auto s1 = u2.get_result();
  REQUIRE_FALSE(s1.is_empty());
  REQUIRE(s1.get_estimate() == Approx(1).margin(RELATIVE_ERROR_FOR_LG_K_11));
  s.update(2);
  u2.update(s);
  u1 = u2; // operator=
  auto s2 = u1.get_result();
  REQUIRE_FALSE(s2.is_empty());
  REQUIRE(s2.get_estimate() == Approx(2).margin(2 * RELATIVE_ERROR_FOR_LG_K_11));
}

TEST_CASE("cpc union: custom seed", "[cpc_union]") {
  cpc_sketch s(11, 123);

  s.update(1);
  s.update(2);
  s.update(3);

  cpc_union u1(11, 123);
  u1.update(s);
  auto r = u1.get_result();
  REQUIRE_FALSE(r.is_empty());
  REQUIRE(r.get_estimate() == Approx(3).margin(3 * RELATIVE_ERROR_FOR_LG_K_11));

  // incompatible seed
  cpc_union u2(11, 234);
  REQUIRE_THROWS_AS(u2.update(s), std::invalid_argument);
}

TEST_CASE("cpc union: large", "[cpc_union]") {
  int key = 0;
  cpc_sketch s(11);
  cpc_union u(11);
  for (int i = 0; i < 1000; i++) {
    cpc_sketch tmp(11);
    for (int j = 0; j < 10000; j++) {
      s.update(key);
      tmp.update(key);
      key++;
    }
    u.update(tmp);
  }
  cpc_sketch r = u.get_result();
  REQUIRE(r.get_num_coupons() == s.get_num_coupons());
  REQUIRE(r.get_estimate() == Approx(s.get_estimate()).margin(s.get_estimate() * RELATIVE_ERROR_FOR_LG_K_11));
}

TEST_CASE("cpc union: reduce k empty", "[cpc_union]") {
  cpc_sketch s(11);
  for (int i = 0; i < 10000; i++) s.update(i);
  cpc_union u(12);
  u.update(s);
  cpc_sketch r = u.get_result();
  REQUIRE(r.get_lg_k() == 11);
  REQUIRE(r.get_estimate() == Approx(10000).margin(10000 * RELATIVE_ERROR_FOR_LG_K_11));
}

TEST_CASE("cpc union: reduce k sparse", "[cpc_union]") {
  cpc_union u(12);

  cpc_sketch s12(12);
  for (int i = 0; i < 100; i++) s12.update(i);
  u.update(s12);

  cpc_sketch s11(11);
  for (int i = 0; i < 1000; i++) s11.update(i);
  u.update(s11);

  cpc_sketch r = u.get_result();
  REQUIRE(r.get_lg_k() == 11);
  REQUIRE(r.get_estimate() == Approx(1000).margin(1000 * RELATIVE_ERROR_FOR_LG_K_11));
}

TEST_CASE("cpc union: reduce k window", "[cpc_union]") {
  cpc_union u(12);

  cpc_sketch s12(12);
  for (int i = 0; i < 500; i++) s12.update(i);
  u.update(s12);

  cpc_sketch s11(11);
  for (int i = 0; i < 1000; i++) s11.update(i);
  u.update(s11);

  cpc_sketch r = u.get_result();
  REQUIRE(r.get_lg_k() == 11);
  REQUIRE(r.get_estimate() == Approx(1000).margin(1000 * RELATIVE_ERROR_FOR_LG_K_11));
}

TEST_CASE("cpc union: moving update", "[cpc_union]") {
  cpc_union u(11);
  cpc_sketch s(11);
  for (int i = 0; i < 100; i++) s.update(i); // sparse
  u.update(std::move(s));
  cpc_sketch r = u.get_result();
  REQUIRE(r.get_estimate() == Approx(100).margin(100 * RELATIVE_ERROR_FOR_LG_K_11));
}



// A sketch that comes out of a union has no HIP estimate, so its bounds come from the ICON estimate.
// The expected values were computed with the DataSketches Java CpcUnion on the same input.
TEST_CASE("cpc union: bounds of the result", "[cpc_union]") {
  struct expected_bounds { uint8_t lg_k; double estimate; double lb[3]; double ub[3]; };
  const expected_bounds cases[] = {
    // lg_k up to 14 uses the measured error table, above it a constant
    {10, 15159.228912348806, {14831.317746546260, 14513.557043381377, 14202.553767187180}, {15488, 15828, 16174}},
    {16, 15005.337536986240, {14964.818701476400, 14924.518102269025, 14884.433980953483}, {15047, 15088, 15129}}
  };
  for (const auto& c: cases) {
    const int lg_k = c.lg_k; // printed as a number, not a character
    CAPTURE(lg_k);
    cpc_sketch s1(c.lg_k), s2(c.lg_k);
    for (uint64_t i = 0; i < 5000; ++i) s1.update(i);
    for (uint64_t i = 5000; i < 15000; ++i) s2.update(i);
    cpc_union u(c.lg_k);
    u.update(s1);
    u.update(s2);
    const auto result = u.get_result();
    // Floating-point results differ slightly between compilers and math libraries
    // (about 4e-12 relative with gcc in the sanitizer builds), so compare within 1e-9.
    // The upper bound is rounded up to a whole number, so it may differ by 1.
    REQUIRE(result.get_estimate() == Approx(c.estimate).epsilon(1e-9));
    for (unsigned kappa = 1; kappa <= 3; ++kappa) {
      CAPTURE(kappa);
      REQUIRE(result.get_lower_bound(kappa) == Approx(c.lb[kappa - 1]).epsilon(1e-9));
      REQUIRE(result.get_upper_bound(kappa) == Approx(c.ub[kappa - 1]).margin(1));
    }
    REQUIRE_THROWS_AS(result.get_lower_bound(0), std::invalid_argument);
    REQUIRE_THROWS_AS(result.get_upper_bound(4), std::invalid_argument);
  }

  // the result of a union of empty sketches has bounds of 0
  cpc_union u(10);
  u.update(cpc_sketch(10));
  const auto empty = u.get_result();
  REQUIRE(empty.get_lower_bound(1) == 0.0);
  REQUIRE(empty.get_upper_bound(1) == 0.0);
}

} /* namespace datasketches */
