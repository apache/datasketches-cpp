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
#include <cmath>
#include <cstring>
#include <limits>

#include "hll.hpp"
#include "fdlibm_log.hpp"

namespace datasketches {

// offsets into the HLL updatable image
static const size_t CUR_MIN_BYTE = 6;
static const size_t NUM_AT_CUR_MIN_INT = 32;
static const size_t HLL_BYTE_ARR_START = 40;

static hll_sketch make(uint8_t lg_k, target_hll_type type, uint64_t lo, uint64_t hi) {
  hll_sketch sk(lg_k, type);
  for (uint64_t i = lo; i < hi; ++i) sk.update(i);
  return sk;
}
static uint32_t num_at_cur_min_of(const hll_sketch::vector_bytes& img) {
  uint32_t v; std::memcpy(&v, img.data() + NUM_AT_CUR_MIN_INT, sizeof(v)); return v;
}

TEST_CASE("hll kxq rebuild: union result is merge-order independent", "[hll_kxq]") {
  // C stays in SET mode, so this exercises the coupon-update path into a gadget with a
  // deferred rebuild pending, which is where the stored curMin/numAtCurMin used to drift
  const hll_sketch a = make(12, HLL_4, 20000, 30364);
  const hll_sketch b = make(10, HLL_8, 5000, 14699);
  const hll_sketch c = make(17, HLL_4, 70000, 72598);
  const hll_sketch* in[3] = {&a, &b, &c};

  auto run = [&](int i, int j, int k) {
    hll_union u(7);
    u.update(*in[i]); u.update(*in[j]); u.update(*in[k]);
    return u.get_result(HLL_8).serialize_updatable();
  };
  const auto ref = run(0, 1, 2);
  REQUIRE(run(0, 2, 1) == ref);
  REQUIRE(run(1, 0, 2) == ref);
  REQUIRE(run(1, 2, 0) == ref);
  REQUIRE(run(2, 0, 1) == ref);
  REQUIRE(run(2, 1, 0) == ref);
}

TEST_CASE("hll kxq rebuild: reading an estimate does not change a later result",
          "[hll_kxq]") {
  // the rebuild is lazy; when it fires must not be observable in the serialized image
  const hll_sketch p = make(13, HLL_8, 0, 50000);
  const hll_sketch q = make(13, HLL_8, 50000, 100000);

  for (uint8_t lg_max_k: {uint8_t(7), uint8_t(8), uint8_t(9)}) {
    hll_union peeked(lg_max_k);
    peeked.update(p); peeked.update(q);
    (void) peeked.get_estimate();                       // forces the rebuild here
    for (uint64_t v = 9000000; v < 9400000; ++v) peeked.update(v);

    hll_union plain(lg_max_k);
    plain.update(p); plain.update(q);
    for (uint64_t v = 9000000; v < 9400000; ++v) plain.update(v);

    REQUIRE(peeked.get_result(HLL_8).serialize_updatable()
            == plain.get_result(HLL_8).serialize_updatable());
  }
}

TEST_CASE("hll kxq rebuild: stored curMin and numAtCurMin agree with the registers",
          "[hll_kxq]") {
  // HLL_8 convention: curMin is always 0 and numAtCurMin counts the zero registers
  const hll_sketch a = make(15, HLL_8, 0, 100000);
  const hll_sketch b = make(8, HLL_8, 100000, 200000);
  hll_union u(8);
  u.update(a); u.update(b);
  const auto img = u.get_result(HLL_8).serialize_updatable();

  uint32_t zeros = 0;
  for (size_t i = HLL_BYTE_ARR_START; i < img.size(); ++i) if (img[i] == 0) ++zeros;

  REQUIRE(img[CUR_MIN_BYTE] == 0);
  REQUIRE(num_at_cur_min_of(img) == zeros);
}

TEST_CASE("hll kxq rebuild: relative error constants are full precision", "[hll_kxq]") {
  // lg_k > 12 uses the closed form rather than the interpolation table, so a constant
  // truncated to seven digits is directly observable in the bounds
  const double hip = std::sqrt(std::log(2.0));                 // sqrt(ln 2)
  const double non_hip = std::sqrt((3.0 * std::log(2.0)) - 1.0); // sqrt(3 ln 2 - 1)

  for (uint8_t lg_k: {uint8_t(13), uint8_t(16), uint8_t(21)}) {
    const double k = static_cast<double>(1 << lg_k);
    for (uint8_t sd = 1; sd <= 3; ++sd) {
      const double got_hip = hll_union::get_rel_err(false, false, lg_k, sd);
      const double got_non = hll_union::get_rel_err(false, true, lg_k, sd);
      REQUIRE(got_hip == Approx(sd * hip / std::sqrt(k)).epsilon(1e-15));
      REQUIRE(got_non == Approx(sd * non_hip / std::sqrt(k)).epsilon(1e-15));
    }
  }
}

TEST_CASE("hll kxq rebuild: log matches the fdlibm reference", "[hll_kxq]") {
  // the linear counting estimator subtracts two nearby harmonic numbers, which amplifies a
  // 1 ULP difference in log() by more than an order of magnitude. Pin log() to fdlibm, the
  // function datasketches-java's StrictMath.log is specified to be. These expected values
  // were taken from Java; the platform libm differs from every one of them.
  struct { int x; uint64_t bits; } expected[] = {
    {     48, 0x400ef8383c50bb74ULL},
    {     74, 0x4011375cd6fcab1cULL},
    {    185, 0x4014e1a4f518c72cULL},
    {    196, 0x40151cca16d7bba8ULL},
    {    299, 0x4016cd411481a020ULL},
    {    308, 0x4016eb9f470ac0b8ULL},
    {    334, 0x40173e9bbe951e9cULL},
    {    343, 0x401759d602a5c3c2ULL},
    {   1261, 0x401c8f031e7e1220ULL},
  };
  for (const auto& e: expected) {
    const double got = fdlibm::log(static_cast<double>(e.x));
    uint64_t bits; std::memcpy(&bits, &got, sizeof(bits));
    REQUIRE(bits == e.bits);
  }

  // edge cases: fdlibm produces these by dividing by a zero constant, which MSVC rejects at
  // compile time, so they are returned directly. Pin the values.
  REQUIRE(fdlibm::log(0.0) == -std::numeric_limits<double>::infinity());
  REQUIRE(fdlibm::log(-0.0) == -std::numeric_limits<double>::infinity());
  REQUIRE(std::isnan(fdlibm::log(-1.0)));
  REQUIRE(fdlibm::log(1.0) == 0.0);
  REQUIRE(fdlibm::log(5e-320) == Approx(-735.2178).epsilon(1e-6)); // subnormal scaling path
}

} /* namespace datasketches */
