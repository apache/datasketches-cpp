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

#include <catch.hpp>
#include <ostream>
#include <cmath>
#include <string>
#include <exception>

namespace datasketches {

TEST_CASE("coupon hash set: check corrupt bytearray", "[coupon_hash_set]") {
  int lgK = 8;
  hll_sketch sk1(lgK);
  for (int i = 0; i < 24; ++i) {
    sk1.update(i);
  }
  auto sketchBytes = sk1.serialize_updatable();
  uint8_t* bytes = sketchBytes.data();
  const size_t size = sketchBytes.size();

  bytes[HllUtil<>::PREAMBLE_INTS_BYTE] = 0;
  // fail in HllSketchImpl
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  // fail in CouponHashSet
  REQUIRE_THROWS_AS(CouponHashSet<std::allocator<uint8_t>>::newSet(bytes, size, std::allocator<uint8_t>()), std::invalid_argument);
  bytes[HllUtil<>::PREAMBLE_INTS_BYTE] = HllUtil<>::HASH_SET_PREINTS;

  bytes[HllUtil<>::SER_VER_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[HllUtil<>::SER_VER_BYTE] = HllUtil<>::SER_VER;

  bytes[HllUtil<>::FAMILY_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[HllUtil<>::FAMILY_BYTE] = HllUtil<>::FAMILY_ID;

  bytes[HllUtil<>::LG_K_BYTE] = 6;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[HllUtil<>::LG_K_BYTE] = lgK;

  uint8_t tmp = bytes[HllUtil<>::MODE_BYTE];
  bytes[HllUtil<>::MODE_BYTE] = 0x10; // HLL_6, LIST
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[HllUtil<>::MODE_BYTE] = tmp;

  tmp = bytes[HllUtil<>::LG_ARR_BYTE];
  bytes[HllUtil<>::LG_ARR_BYTE] = 0;
  hll_sketch::deserialize(bytes, size);
  // should work fine despite the corruption
  bytes[HllUtil<>::LG_ARR_BYTE] = tmp;

  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size - 1), std::out_of_range);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, 3), std::out_of_range);
}

TEST_CASE("coupon hash set: check corrupt stream", "[coupon_hash_set]") {
  int lgK = 9;
  hll_sketch sk1(lgK);
  for (int i = 0; i < 24; ++i) {
    sk1.update(i);
  }
  std::stringstream ss;
  sk1.serialize_compact(ss);

  ss.seekp(HllUtil<>::PREAMBLE_INTS_BYTE);
  ss.put(0);
  ss.seekg(0);
  // fail in HllSketchImpl
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  // fail in CouponHashSet
  REQUIRE_THROWS_AS(CouponHashSet<std::allocator<uint8_t>>::newSet(ss, std::allocator<uint8_t>()), std::invalid_argument);
  ss.seekp(HllUtil<>::PREAMBLE_INTS_BYTE);
  ss.put(HllUtil<>::HASH_SET_PREINTS);

  ss.seekp(HllUtil<>::SER_VER_BYTE);
  ss.put(0);
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(HllUtil<>::SER_VER_BYTE);
  ss.put(HllUtil<>::SER_VER);

  ss.seekp(HllUtil<>::FAMILY_BYTE);
  ss.put(0);
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(HllUtil<>::FAMILY_BYTE);
  ss.put(HllUtil<>::FAMILY_ID);

  ss.seekg(HllUtil<>::MODE_BYTE);
  uint8_t tmp = ss.get();
  ss.seekp(HllUtil<>::MODE_BYTE);
  ss.put(0x22); // HLL_8, HLL
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  ss.seekp(HllUtil<>::MODE_BYTE);
  ss.put(tmp);

  ss.seekg(HllUtil<>::LG_ARR_BYTE);
  tmp = ss.get();
  ss.seekp(HllUtil<>::LG_ARR_BYTE);
  ss.put(0);
  ss.seekg(0);
  hll_sketch::deserialize(ss);
  // should work fine despite the corruption
  ss.seekp(HllUtil<>::LG_ARR_BYTE);
  ss.put(tmp);
}


} // namespace datasketches
