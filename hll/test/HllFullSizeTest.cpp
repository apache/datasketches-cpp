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

#include "hll.hpp"

namespace datasketches {

// preamble byte 0 is preInts, which identifies the current mode
static const uint8_t LIST_PREINTS = 2;
static const uint8_t HLL_PREINTS = 10;
// preamble byte 5 is the flags byte; bit 32 is reserved and must never be written
static const size_t FLAGS_BYTE = 5;
static const uint8_t RESERVED_BIT_32 = 32;

static uint8_t preints_of(const hll_sketch& sk) {
  return sk.serialize_compact()[0];
}

static hll_sketch make(uint8_t lg_k, target_hll_type type, bool full_size, uint64_t n) {
  hll_sketch sk(lg_k, type, full_size);
  for (uint64_t i = 0; i < n; ++i) sk.update(i);
  return sk;
}

TEST_CASE("hll full size: reserved bit 32 is never written", "[hll_full_size]") {
  for (auto type: {HLL_4, HLL_6, HLL_8}) {
    for (bool full_size: {false, true}) {
      for (uint64_t n: {uint64_t(0), uint64_t(5), uint64_t(5000)}) {
        const hll_sketch sk = make(8, type, full_size, n);
        REQUIRE((sk.serialize_compact()[FLAGS_BYTE] & RESERVED_BIT_32) == 0);
        REQUIRE((sk.serialize_updatable()[FLAGS_BYTE] & RESERVED_BIT_32) == 0);
      }
    }
  }
}

TEST_CASE("hll full size: start_full_size starts in HLL mode", "[hll_full_size]") {
  REQUIRE(preints_of(make(8, HLL_8, true, 0)) == HLL_PREINTS);
  REQUIRE(preints_of(make(8, HLL_8, false, 0)) == LIST_PREINTS);
}

TEST_CASE("hll full size: reset takes the mode as an argument", "[hll_full_size]") {
  for (auto type: {HLL_4, HLL_6, HLL_8}) {
    // full size is not remembered: a plain reset() returns to coupon collection mode
    hll_sketch sk = make(8, type, true, 5000);
    sk.reset();
    REQUIRE(sk.is_empty());
    REQUIRE(sk.get_estimate() == 0.0);
    REQUIRE(preints_of(sk) == LIST_PREINTS);

    // ...and must be asked for explicitly
    hll_sketch sk2 = make(8, type, false, 5000);
    sk2.reset(true);
    REQUIRE(sk2.is_empty());
    REQUIRE(sk2.get_estimate() == 0.0);
    REQUIRE(preints_of(sk2) == HLL_PREINTS);
    REQUIRE(sk2.get_lg_config_k() == 8);
    REQUIRE(sk2.get_target_type() == type);

    // reset(false) is the same as reset()
    hll_sketch sk3 = make(8, type, true, 5000);
    sk3.reset(false);
    REQUIRE(preints_of(sk3) == LIST_PREINTS);
  }
}

TEST_CASE("hll full size: a reset sketch is usable again", "[hll_full_size]") {
  hll_sketch sk = make(8, HLL_8, false, 100);
  sk.reset(true);
  for (uint64_t i = 0; i < 1000; ++i) sk.update(i);
  REQUIRE_FALSE(sk.is_empty());
  REQUIRE(sk.get_estimate() == Approx(1000).epsilon(0.2));
}

TEST_CASE("hll full size: an image with the reserved bit set is read as if it were clear",
          "[hll_full_size]") {
  // an image produced by another implementation may have bit 32 set: datasketches-java uses it
  // as its union rebuild flag. It must not change how this implementation reads the sketch.
  for (auto type: {HLL_4, HLL_6, HLL_8}) {
    const hll_sketch sk = make(8, type, false, 5000);
    auto bytes = sk.serialize_updatable();
    auto tampered = bytes;
    tampered[FLAGS_BYTE] |= RESERVED_BIT_32;

    const hll_sketch clean = hll_sketch::deserialize(bytes.data(), bytes.size());
    hll_sketch tainted = hll_sketch::deserialize(tampered.data(), tampered.size());

    REQUIRE(tainted.get_estimate() == clean.get_estimate());
    REQUIRE(tainted.serialize_updatable() == bytes); // the bit is not propagated back out
    tainted.reset();
    REQUIRE(preints_of(tainted) == LIST_PREINTS);    // and does not alter reset() behaviour
  }
}

TEST_CASE("hll full size: a union never inherits full size from an input sketch",
          "[hll_full_size]") {
  // the gadget is an internal detail; unioning a full-size sketch must not change how the
  // union resets, nor put the reserved bit into the union's result
  const hll_sketch full = make(8, HLL_8, true, 5000);

  hll_union u(8);
  u.update(full);
  const hll_sketch result = u.get_result(HLL_8);
  REQUIRE((result.serialize_compact()[FLAGS_BYTE] & RESERVED_BIT_32) == 0);
  REQUIRE((result.serialize_updatable()[FLAGS_BYTE] & RESERVED_BIT_32) == 0);

  u.reset();
  REQUIRE(u.is_empty());
  REQUIRE(preints_of(u.get_result(HLL_8)) == LIST_PREINTS);

  // same through the rvalue overload, which moves the sketch into the gadget
  hll_union u2(8);
  u2.update(make(8, HLL_8, true, 5000));
  u2.reset();
  REQUIRE(u2.is_empty());
  REQUIRE(preints_of(u2.get_result(HLL_8)) == LIST_PREINTS);
}

} /* namespace datasketches */
