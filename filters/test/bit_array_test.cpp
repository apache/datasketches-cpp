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
//#include <sstream>
//#include <fstream>
//#include <stdexcept>

#include "bit_array.hpp"

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

TEST_CASE("bit_array: invalid num_bits", "[bit_array]") {
  REQUIRE_THROWS_AS(bit_array(0), std::invalid_argument);
  REQUIRE_THROWS_AS(bit_array(1L << 60), std::invalid_argument);
}

TEST_CASE("bit_array: construction", "[bit_array]") {
  bit_array ba(64);
  REQUIRE(ba.get_capacity() == 64);
  REQUIRE(ba.get_num_bits_set() == 0);
  REQUIRE(ba.is_empty());
  REQUIRE(!ba.is_dirty());
}

TEST_CASE("bit_array: basic operation", "[bit_array]") {
  bit_array ba(128);
  REQUIRE(ba.get_and_set_bit(1) == false);
  REQUIRE(ba.get_and_set_bit(2) == false);
  for (int i = 4; i < 64; i <<= 1) {
    REQUIRE(ba.get_and_set_bit(64 + i) == false);
  }

  REQUIRE(ba.get_num_bits_set() == 6);
  REQUIRE(ba.get_bit(68));
  REQUIRE(!ba.is_empty());

  REQUIRE(ba.get_bit(5) == false);
  ba.set_bit(5);
  REQUIRE(ba.get_and_set_bit(5));
  REQUIRE(ba.get_num_bits_set() == 7);

  ba.clear_bit(5);
  REQUIRE(ba.get_bit(5) == false);
  REQUIRE(ba.get_num_bits_set() == 6);

  ba.reset();
  REQUIRE(ba.is_empty());
  REQUIRE(ba.get_num_bits_set() == 0);

  ba.set_bit(35);
  REQUIRE(ba.get_and_set_bit(35));
  ba.assign_bit(35, false);
  REQUIRE(ba.get_bit(35) == false);
  ba.assign_bit(35, true);
  REQUIRE(ba.get_bit(35));

  REQUIRE(ba.to_string().length() > 0);
}

TEST_CASE("bit_array: inversion", "[bit_array]") {
  size_t num_bits = 1024;
  bit_array ba(num_bits);
  for (size_t i = 0; i < num_bits; i += num_bits / 8) {
    ba.get_and_set_bit(i);
  }
  REQUIRE(ba.get_bit(0));

  size_t num_bits_set = ba.get_num_bits_set();
  ba.invert();
  REQUIRE(ba.get_num_bits_set() == num_bits - num_bits_set);
  REQUIRE(ba.get_bit(0) == false);

  // update to make dirty and invert again
  ba.set_bit(0);
  ba.invert();
  REQUIRE(ba.get_num_bits_set() == num_bits_set - 1);
  REQUIRE(ba.get_bit(0) == false);
}

TEST_CASE("bit_array: invalid union and intersection", "[bit_array]") {
  bit_array ba1(64);
  bit_array ba2(128);
  REQUIRE_THROWS_AS(ba1.union_with(ba2), std::invalid_argument);
  REQUIRE_THROWS_AS(ba1.intersect(ba2), std::invalid_argument);
}

TEST_CASE("bit_array: intersection and union", "[bit_array]") {
  bit_array ba1(64);
  bit_array ba2(64);
  bit_array ba3(64);

  size_t n = 10;
  for (size_t i = 0; i < n; ++i) {
    ba1.get_and_set_bit(i);
    ba2.get_and_set_bit(i + (n / 2));
    ba3.get_and_set_bit(2 * i);
  }
  REQUIRE(ba1.get_num_bits_set() == n);
  REQUIRE(ba2.get_num_bits_set() == n);
  REQUIRE(ba3.get_num_bits_set() == n);

  ba1.intersect(ba2);
  REQUIRE(ba1.get_num_bits_set() == n / 2);

  ba3.union_with(ba2);
  REQUIRE(ba3.get_num_bits_set() == 3 * n / 2);
}

} // namespace datasketches
