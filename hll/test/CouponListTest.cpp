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
#include <ostream>
#include <sstream>
#include <cmath>
#include <string>
#include <exception>

#include "hll.hpp"
#include "CouponList.hpp"
#include "HllUtil.hpp"

namespace datasketches {

void println_string(std::string str) {
  //std::cout << str << std::endl;
}

TEST_CASE("coupon list: check iterator", "[coupon_list]") {
  int lgConfigK = 8;
  CouponList<std::allocator<uint8_t>> cl(lgConfigK, HLL_4, LIST, std::allocator<uint8_t>());
  for (int i = 1; i <= 7; ++i) { cl.couponUpdate(HllUtil<>::pair(i, i)); } // not hashes but distinct values
  const int mask = (1 << lgConfigK) - 1;
  int idx = 0;
  auto itr = cl.begin(false);
  while (itr != cl.end()) {
    int key = HllUtil<>::getLow26(*itr);
    int val = HllUtil<>::getValue(*itr);
    int slot = HllUtil<>::getLow26(*itr) & mask;
    std::ostringstream oss;
    oss << "Idx: " << idx << ", Key: " << key << ", Val: " << val
        << ", Slot: " << slot;
    println_string(oss.str());
    REQUIRE(val == idx + 1);
    ++itr;
    ++idx;
  }
}

TEST_CASE("coupon list: check duplicates and misc", "[coupon_list]") {
  int lgConfigK = 8;
  hll_sketch sk(lgConfigK);

  for (int i = 1; i <= 7; ++i) {
    sk.update(i);
    sk.update(i);
  }
  REQUIRE(sk.get_composite_estimate() == Approx(7.0).epsilon(0.1));

  sk.update(8);
  sk.update(8);
  REQUIRE(sk.get_composite_estimate() == Approx(8.0).epsilon(0.1));

  for (int i = 9; i <= 25; ++i) {
    sk.update(i);
    sk.update(i);
  }
  REQUIRE(sk.get_composite_estimate() == Approx(25.0).epsilon(0.1));

  double relErr = sk.get_rel_err(true, true, 4, 1);
  REQUIRE(relErr < 0.0);
}

static void serializeDeserialize(const int lgK) {
  hll_sketch sk1(lgK);

  int u = (lgK < 8) ? 7 : (((1 << (lgK - 3))/ 4) * 3);
  for (int i = 0; i < u; ++i) {
    sk1.update(i);
  }
  double est1 = sk1.get_estimate();
  REQUIRE(est1 == Approx(u).margin(u * 1e-4));

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  sk1.serialize_compact(ss);
  hll_sketch sk2 = hll_sketch::deserialize(ss);
  double est2 = sk2.get_estimate();
  REQUIRE(est1 == est2);

  ss.str(std::string());
  ss.clear();

  sk1.serialize_updatable(ss);
  sk2 = hll_sketch::deserialize(ss);
  est2 = sk2.get_estimate();
  REQUIRE(est1 == est2);
}

TEST_CASE("coupon list: check serialize deserialize", "[coupon_list]") {
  serializeDeserialize(7);
  serializeDeserialize(21);
}

TEST_CASE("coupon list: check corrupt bytearray data", "[coupon_list]") {
  int lgK = 6;
  hll_sketch sk1(lgK);
  sk1.update(1);
  sk1.update(2);
  auto sketchBytes = sk1.serialize_compact();
  uint8_t* bytes = sketchBytes.data();
  const size_t size = sketchBytes.size();

  bytes[HllUtil<>::PREAMBLE_INTS_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  REQUIRE_THROWS_AS(CouponList<std::allocator<uint8_t>>::newList(bytes, size, std::allocator<uint8_t>()), std::invalid_argument);

  bytes[HllUtil<>::PREAMBLE_INTS_BYTE] = HllUtil<>::LIST_PREINTS;

  bytes[HllUtil<>::SER_VER_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[HllUtil<>::SER_VER_BYTE] = HllUtil<>::SER_VER;

  bytes[HllUtil<>::FAMILY_BYTE] = 0;
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[HllUtil<>::FAMILY_BYTE] = HllUtil<>::FAMILY_ID;

  uint8_t tmp = bytes[HllUtil<>::MODE_BYTE];
  bytes[HllUtil<>::MODE_BYTE] = 0x01; // HLL_4, SET
  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size), std::invalid_argument);
  bytes[HllUtil<>::MODE_BYTE] = tmp;

  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, size - 1), std::out_of_range);

  REQUIRE_THROWS_AS(hll_sketch::deserialize(bytes, 3), std::out_of_range);
}

TEST_CASE("coupon list: check corrupt stream data", "[coupon_list]") {
  int lgK = 6;
  hll_sketch sk1(lgK);
  sk1.update(1);
  sk1.update(2);
  std::stringstream ss;
  sk1.serialize_compact(ss);

  ss.seekp(HllUtil<>::PREAMBLE_INTS_BYTE);
  ss.put(0);
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
  REQUIRE_THROWS_AS(CouponList<std::allocator<uint8_t>>::newList(ss, std::allocator<uint8_t>()), std::invalid_argument);
  ss.seekp(HllUtil<>::PREAMBLE_INTS_BYTE);
  ss.put(HllUtil<>::LIST_PREINTS);

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

  ss.seekp(HllUtil<>::MODE_BYTE);
  ss.put(0x22); // HLL_8, HLL
  ss.seekg(0);
  REQUIRE_THROWS_AS(hll_sketch::deserialize(ss), std::invalid_argument);
}

} /* namespace datasketches */
