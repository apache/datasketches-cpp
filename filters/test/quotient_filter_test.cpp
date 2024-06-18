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
#include <fstream>

#include <quotient_filter.hpp>

namespace datasketches {

TEST_CASE("empty", "[quotient_filter]") {
  quotient_filter f(10, 9);
  REQUIRE(f.get_lg_q() == 10);
  REQUIRE(f.get_num_bits_per_entry() == 9);
  REQUIRE(f.get_num_entries() == 0);
}

TEST_CASE("one entry", "[quotient_filter]") {
  quotient_filter f(4, 6);
  REQUIRE_FALSE(f.query(1));
  REQUIRE(f.update(1));
  REQUIRE(f.query(1));
  REQUIRE(f.get_num_entries() == 1);
  REQUIRE_FALSE(f.update(1));
}

TEST_CASE("several entries", "[quotient_filter]") {
  quotient_filter f(5, 9);
  f.update(1);
  f.update(2);
  f.update(3);
//  std::cout << f.to_string(true);
  REQUIRE(f.query(1));
  REQUIRE(f.query(2));
  REQUIRE(f.query(3));
  REQUIRE(f.get_num_entries() == 3);
}

TEST_CASE("many entries no expansion 1", "[quotient_filter]") {
  quotient_filter f(4, 9);
  const size_t n = 12;
  for (size_t i = 0; i < n; ++i) f.update(i);
//  std::cout << f.to_string(true);

  REQUIRE(f.get_num_expansions() == 0);
  REQUIRE(f.get_num_entries() == n);
  size_t positives = 0;
  for (size_t i = 0; i < n; ++i) {
    if (f.query(i)) ++positives;
  }
  REQUIRE(positives == n);
  // query novel keys
  positives = 0;
  for (size_t i = 0; i < n; ++i) if (f.query(i + n)) ++positives;
  REQUIRE(positives < 2);
}

TEST_CASE("many entries no expansion 2", "[quotient_filter]") {
  quotient_filter f(6, 12);
  const size_t n = 40;
  for (size_t i = 0; i < n; ++i) f.update(i);
//  std::cout << f.to_string(true);
  REQUIRE(f.get_num_expansions() == 0);
  REQUIRE(f.get_num_entries() == n);
  size_t positives = 0;
  for (size_t i = 0; i < n; ++i) {
    if (f.query(i)) ++positives;
  }
  REQUIRE(positives == n);
  // query novel keys
  positives = 0;
  for (size_t i = 0; i < n; ++i) if (f.query(i + n)) ++positives;
  REQUIRE(positives == 0);
}

TEST_CASE("many more entries no expansion", "[quotient_filter]") {
  quotient_filter f(16, 16);
  const size_t n = 30000;
  for (size_t i = 0; i < n; ++i) f.update(i);
//  std::cout << f.to_string(true);
  REQUIRE(f.get_num_expansions() == 0);
  REQUIRE(f.get_num_entries() > n * 0.999); // allow a few hash collisions

  // query the same keys
  size_t positives = 0;
  for (size_t i = 0; i < n; ++i) if (f.query(i)) ++positives;
  REQUIRE(positives == n);

  // query novel keys
  positives = 0;
  for (size_t i = 0; i < n; ++i) if (f.query(i + n)) ++positives;
  REQUIRE(positives < 2);
}

TEST_CASE("small expansion", "[quotient_filter]") {
  quotient_filter f(5, 12);
  const size_t n = 30;
  for (size_t i = 0; i < n; ++i) f.update(i);
  std::cout << f.to_string(true);
  REQUIRE(f.get_num_expansions() == 1);
  REQUIRE(f.get_num_entries() == n);

  // query the same keys
  size_t positives = 0;
  for (size_t i = 0; i < n; ++i) if (f.query(i)) ++positives;
  REQUIRE(positives == n);

  // query novel keys
  positives = 0;
  for (size_t i = 0; i < n; ++i) if (f.query(i + n)) ++positives;
  REQUIRE(positives < 2);
}

TEST_CASE("expansion", "[quotient_filter]") {
  quotient_filter f(16, 16);
  const size_t n = 60000;
  for (size_t i = 0; i < n; ++i) f.update(i);
  std::cout << f.to_string();
//  std::cout << f.to_string(true);
  REQUIRE(f.get_num_expansions() == 1);
  REQUIRE(f.get_num_entries() > n * 0.999); // allow a few hash collisions

  // query the same keys
  size_t positives = 0;
  for (size_t i = 0; i < n; ++i) if (f.query(i)) ++positives;
  REQUIRE(positives == n);

  // query novel keys
  positives = 0;
  for (size_t i = 0; i < n; ++i) if (f.query(i + n)) ++positives;
  REQUIRE(positives < 7);

  for (size_t i = 0; i < n * 2; ++i) if (f.update(i + n)) ++positives;
  std::cout << f.to_string();
}

TEST_CASE("serialize", "[quotient_filter]") {
  quotient_filter f(4, 9);
  for (int i = 0; i < 12; ++i) f.update(i);

  std::ofstream os("quotient_filter_4_9_cpp.sk", std::ios::binary);
  f.serialize(os);
}

// inverse golden ratio (0.618... of max uint64_t)
static const uint64_t IGOLDEN64 = 0x9e3779b97f4a7c13ULL;

TEST_CASE("pack unpack bits") {
  for (uint8_t num_bits = 1; num_bits <= 63; ++num_bits) {
    int n = 8;
    const uint64_t mask = (1ULL << num_bits) - 1;
    std::vector<uint64_t> input(n, 0);
    const uint64_t igolden64 = IGOLDEN64;
    uint64_t value = 0xaa55aa55aa55aa55ULL; // arbitrary starting value
    for (int i = 0; i < n; ++i) {
      input[i] = value & mask;
      value += igolden64;
    }
    std::vector<uint8_t> bytes(n * sizeof(uint64_t), 0);
    for (int i = 0; i < n; ++i) {
      const size_t bit_index = i * num_bits;
      const size_t byte_offset = bit_index >> 3;
      const uint8_t bit_offset = bit_index & 7;
      put_bits(input[i], num_bits, bytes.data() + byte_offset, bit_offset);
    }

    std::vector<uint64_t> output(n, 0);
    for (int i = 0; i < n; ++i) {
      const size_t bit_index = i * num_bits;
      const size_t byte_offset = bit_index >> 3;
      const uint8_t bit_offset = bit_index & 7;
      output[i] = get_bits(num_bits, bytes.data() + byte_offset, bit_offset);
    }
    for (int i = 0; i < n; ++i) {
      REQUIRE(input[i] == output[i]);
    }
  }
}

} /* namespace datasketches */
