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
#include <algorithm>

#include "bit_array_ops.hpp"

namespace datasketches {

TEST_CASE("bit_array: basic operation", "[bit_array]") {
  uint8_t* data = new uint8_t[16];
  std::fill_n(data, 16, 0);
  REQUIRE(bit_array_ops::get_and_set_bit(data, 1) == false);
  REQUIRE(bit_array_ops::get_and_set_bit(data, 2) == false);
  for (int i = 4; i < 64; i <<= 1) {
    REQUIRE(bit_array_ops::get_and_set_bit(data, 64 + i) == false);
  }

  REQUIRE(bit_array_ops::count_num_bits_set(data, 16) == 6);
  REQUIRE(bit_array_ops::get_bit(data, 68));

  REQUIRE(bit_array_ops::get_bit(data, 5) == false);
  bit_array_ops::set_bit(data, 5);
  REQUIRE(bit_array_ops::get_and_set_bit(data, 5));
  REQUIRE(bit_array_ops::count_num_bits_set(data, 16) == 7);

  bit_array_ops::clear_bit(data, 5);
  REQUIRE(bit_array_ops::get_bit(data, 5) == false);
  REQUIRE(bit_array_ops::count_num_bits_set(data, 16) == 6);

  std::fill(data, data + 16, 0);
  REQUIRE(bit_array_ops::count_num_bits_set(data, 16) == 0);

  bit_array_ops::set_bit(data, 35);
  REQUIRE(bit_array_ops::get_and_set_bit(data, 35));
  bit_array_ops::assign_bit(data, 35, false);
  REQUIRE(bit_array_ops::get_bit(data, 35) == false);
  bit_array_ops::assign_bit(data, 35, true);
  REQUIRE(bit_array_ops::get_bit(data, 35));

  delete [] data;
}

TEST_CASE("bit_array: inversion", "[bit_array]") {
  size_t num_bits = 1024;
  uint8_t* data = new uint8_t[num_bits / 8];
  std::fill_n(data, num_bits / 8, 0);
  for (size_t i = 0; i < num_bits; i += num_bits / 8) {
    bit_array_ops::get_and_set_bit(data, i);
  }
  REQUIRE(bit_array_ops::get_bit(data, 0));

  size_t num_bits_set = bit_array_ops::count_num_bits_set(data, num_bits / 8);
  bit_array_ops::invert(data, num_bits / 8);
  REQUIRE(bit_array_ops::count_num_bits_set(data, num_bits / 8) == num_bits - num_bits_set);
  REQUIRE(bit_array_ops::get_bit(data, 0) == false);

  delete [] data;
}

TEST_CASE("bit_array: intersection and union", "[bit_array]") {
  uint8_t* data1 = new uint8_t[8];
  uint8_t* data2 = new uint8_t[8];
  uint8_t* data3 = new uint8_t[8];
  std::fill_n(data1, 8, 0);
  std::fill_n(data2, 8, 0);
  std::fill_n(data3, 8, 0);

  size_t n = 10;
  for (size_t i = 0; i < n; ++i) {
    bit_array_ops::get_and_set_bit(data1, i);
    bit_array_ops::get_and_set_bit(data2, i + (n / 2));
    bit_array_ops::get_and_set_bit(data3, 2 * i);
  }
  REQUIRE(bit_array_ops::count_num_bits_set(data1, 8) == n);
  REQUIRE(bit_array_ops::count_num_bits_set(data2, 8) == n);
  REQUIRE(bit_array_ops::count_num_bits_set(data3, 8) == n);

  bit_array_ops::intersect(data1, data2, 8);
  REQUIRE(bit_array_ops::count_num_bits_set(data1, 8) == n / 2);

  bit_array_ops::union_with(data3, data2, 8);
  REQUIRE(bit_array_ops::count_num_bits_set(data3, 8) == 3 * n / 2);

  delete [] data1;
  delete [] data2;
  delete [] data3;
}

} // namespace datasketches
