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

#include "hll.hpp"
#include "CouponHashSet.hpp"
#include "HllUtil.hpp"

#include <catch2/catch.hpp>
#include <cstring>
#include <limits>
#include <ostream>
#include <sstream>
#include <cmath>
#include <string>
#include <exception>
#include <stdexcept>

namespace datasketches {

static hll_sketch::vector_bytes make_updatable_set(uint8_t lg_k, int count) {
  hll_sketch sketch(lg_k);
  for (int i = 0; i < count; ++i) sketch.update(i);
  return sketch.serialize_updatable();
}

static void set_serialized_count(hll_sketch::vector_bytes& image, uint32_t count) {
  std::memcpy(image.data() + hll_constants::HASH_SET_COUNT_INT, &count, sizeof(count));
}

static hll_sketch deserialize_set(const hll_sketch::vector_bytes& image, bool stream) {
  if (!stream) return hll_sketch::deserialize(image.data(), image.size());
  std::istringstream input(std::string(image.begin(), image.end()));
  return hll_sketch::deserialize(input);
}

TEST_CASE("coupon hash set: check corrupt bytearray", "[coupon_hash_set]") {
  uint8_t lgK = 8;
  hll_sketch sk1(lgK);
  for (int i = 0; i < 24; ++i) {
    sk1.update(i);
  }
  auto sketchBytes = sk1.serialize_updatable();
  uint8_t* bytes = sketchBytes.data();
  const size_t size = sketchBytes.size();

  bytes[hll_constants::PREAMBLE_INTS_BYTE] = 0;
  // fail in HllSketchImpl
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  // fail in CouponHashSet
  REQUIRE_THROWS_AS(CouponHashSet<std::allocator<uint8_t>>::newSet(bytes, size, std::allocator<uint8_t>()), std::invalid_argument);
  bytes[hll_constants::PREAMBLE_INTS_BYTE] = hll_constants::HASH_SET_PREINTS;

  bytes[hll_constants::SER_VER_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::SER_VER_BYTE] = hll_constants::SER_VER;

  bytes[hll_constants::FAMILY_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::FAMILY_BYTE] = hll_constants::FAMILY_ID;

  bytes[hll_constants::LG_K_BYTE] = 6;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::LG_K_BYTE] = lgK;

  uint8_t tmp = bytes[hll_constants::MODE_BYTE];
  bytes[hll_constants::MODE_BYTE] = 0x10; // HLL_6, LIST
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[hll_constants::MODE_BYTE] = tmp;

  tmp = bytes[hll_constants::LG_ARR_BYTE];
  bytes[hll_constants::LG_ARR_BYTE] = 0;
  hll_sketch::deserialize(bytes, size);
  // should work fine despite the corruption
  bytes[hll_constants::LG_ARR_BYTE] = tmp;

  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size - 1), std::out_of_range);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, 3), std::out_of_range);
}

TEST_CASE("coupon hash set: check corrupt stream", "[coupon_hash_set]") {
  uint8_t lgK = 9;
  hll_sketch sk1(lgK);
  for (int i = 0; i < 24; ++i) {
    sk1.update(i);
  }
  std::stringstream ss;
  sk1.serialize_compact(ss);

  ss.seekp(hll_constants::PREAMBLE_INTS_BYTE);
  ss.put(0);
  ss.seekg(0);
  // fail in HllSketchImpl
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  // fail in CouponHashSet
  REQUIRE_THROWS_AS(CouponHashSet<std::allocator<uint8_t>>::newSet(ss, std::allocator<uint8_t>()), std::invalid_argument);
  ss.seekp(hll_constants::PREAMBLE_INTS_BYTE);
  ss.put(hll_constants::HASH_SET_PREINTS);

  ss.seekp(hll_constants::SER_VER_BYTE);
  ss.put(0);
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(hll_constants::SER_VER_BYTE);
  ss.put(hll_constants::SER_VER);

  ss.seekp(hll_constants::FAMILY_BYTE);
  ss.put(0);
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(hll_constants::FAMILY_BYTE);
  ss.put(hll_constants::FAMILY_ID);

  ss.seekg(hll_constants::MODE_BYTE);
  auto tmp = ss.get();
  ss.seekp(hll_constants::MODE_BYTE);
  ss.put(0x22); // HLL_8, HLL
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(hll_constants::MODE_BYTE);
  ss.put((char)tmp);

  ss.seekg(hll_constants::LG_ARR_BYTE);
  tmp = ss.get();
  ss.seekp(hll_constants::LG_ARR_BYTE);
  ss.put(0);
  ss.seekg(0);
  hll_sketch::deserialize(ss);
  // should work fine despite the corruption
  ss.seekp(hll_constants::LG_ARR_BYTE);
  ss.put((char)tmp);
}

TEST_CASE("coupon hash set: validates updatable tables", "[coupon_hash_set]") {
  const bool stream = GENERATE(false, true);
  CAPTURE(stream);
  auto image = make_updatable_set(8, 24);

  SECTION("rejects a table larger than the precision allows") {
    image[hll_constants::LG_ARR_BYTE] = 6;
    image.resize(hll_constants::HASH_SET_INT_ARR_START + 64 * sizeof(uint32_t), 0);
    REQUIRE_THROWS_AS(deserialize_set(image, stream), std::invalid_argument);
  }
  SECTION("rejects too many occupied slots for the declared count") {
    set_serialized_count(image, 23);
    REQUIRE_THROWS_AS(deserialize_set(image, stream), std::invalid_argument);
  }
  SECTION("rejects too few occupied slots for the declared count") {
    image = make_updatable_set(8, 23);
    set_serialized_count(image, 24);
    REQUIRE_THROWS_AS(deserialize_set(image, stream), std::invalid_argument);
  }
  SECTION("rejects a count above the table load limit before reading the table") {
    image[hll_constants::LG_K_BYTE] = 9;
    set_serialized_count(image, 25);
    image.resize(hll_constants::HASH_SET_INT_ARR_START);
    REQUIRE_THROWS_AS(deserialize_set(image, stream), std::invalid_argument);
  }
}

TEST_CASE("coupon hash set: rejects excessive legacy count", "[coupon_hash_set]") {
  const bool stream = GENERATE(false, true);
  CAPTURE(stream);
  auto image = make_updatable_set(8, 24);
  image[hll_constants::LG_ARR_BYTE] = 0;
  set_serialized_count(image, std::numeric_limits<uint32_t>::max());
  REQUIRE_THROWS_AS(deserialize_set(image, stream), std::invalid_argument);
}

TEST_CASE("coupon hash set: restores valid updatable tables", "[coupon_hash_set]") {
  const bool stream = GENERATE(false, true);
  CAPTURE(stream);
  SECTION("at exactly 75 percent occupancy") {
    const auto image = make_updatable_set(8, 24);
    REQUIRE(deserialize_set(image, stream).serialize_updatable() == image);
  }
  SECTION("with a missing size exponent in a grown table") {
    const auto image = make_updatable_set(9, 25);
    auto legacy_image = image;
    legacy_image[hll_constants::LG_ARR_BYTE] = 0;
    REQUIRE(deserialize_set(legacy_image, stream).serialize_updatable() == image);
  }
}

TEST_CASE("coupon hash set: reports truncated stream reads", "[coupon_hash_set]") {
  auto image = make_updatable_set(8, 24);
  SECTION("header") {
    image.resize(hll_constants::MODE_BYTE);
  }
  SECTION("coupon count") {
    image.resize(hll_constants::HASH_SET_INT_ARR_START - 1);
  }
  SECTION("updatable table") {
    image.resize(hll_constants::HASH_SET_INT_ARR_START + sizeof(uint32_t));
  }
  SECTION("compact coupon") {
    image = deserialize_set(image, false).serialize_compact();
    image.pop_back();
  }
  REQUIRE_THROWS_AS(deserialize_set(image, true), std::runtime_error);
}

} // namespace datasketches
