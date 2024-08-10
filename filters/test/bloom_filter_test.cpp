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

#include "bloom_filter.hpp"

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

TEST_CASE("bloom_filter: invalid constructor args", "[bloom_filter]") {
  REQUIRE_THROWS_AS(bloom_filter_builder::create_by_size(0, 4), std::invalid_argument);
  REQUIRE_THROWS_AS(bloom_filter_builder::create_by_size(1L << 60, 4), std::invalid_argument);
  REQUIRE_THROWS_AS(bloom_filter_builder::create_by_size(65535, 0), std::invalid_argument);
}

TEST_CASE("bloom_filter: standard constructors", "[bloom_filter]") {
  uint64_t num_items = 4000;
  double fpp = 0.01;

  uint64_t num_bits = bloom_filter_builder::suggest_num_filter_bits(num_items, fpp);
  uint16_t num_hashes = bloom_filter_builder::suggest_num_hashes(num_items, num_bits);
  uint64_t seed = 89023;

  auto bf = bloom_filter_builder::create_by_size(num_bits, num_hashes, seed);
  uint64_t adjusted_num_bits = (num_bits + 63) & ~0x3F; // round up to the nearest multiple of 64
  REQUIRE(bf.get_capacity() == adjusted_num_bits);
  REQUIRE(bf.get_num_hashes() == num_hashes);
  REQUIRE(bf.is_empty());
}

TEST_CASE("bloom_filter: basic operations", "[bloom_filter]") {
  uint64_t num_bits = 8192;
  uint16_t num_hashes = 3;

  auto bf = bloom_filter_builder::create_by_size(num_bits, num_hashes);
  REQUIRE(bf.is_empty());
  REQUIRE(bf.get_capacity() == num_bits); // num_bits is multiple of 64 so should be exact
  REQUIRE(bf.get_num_hashes() == num_hashes);
  REQUIRE(bf.get_bits_used() == 0);

  uint64_t n = 1000;
  for (uint64_t i = 0; i < n; ++i) {
    bf.query_and_update(i);
  }

  REQUIRE(!bf.is_empty());
  // these assume the filter isn't too close to capacity
  REQUIRE(bf.get_bits_used() <= n * num_hashes);
  REQUIRE(bf.get_bits_used() >= n * (num_hashes - 1));

  uint32_t num_found = 0;
  for (uint64_t i = 0; i < n; ++i) {
    if (bf.query(i)) {
      ++num_found;
    }
  }
  REQUIRE(num_found >= n);
  REQUIRE(num_found < 1.1 * n);

  bf.reset();
  // repeat initial tests from above
  REQUIRE(bf.is_empty());
  REQUIRE(bf.get_capacity() == num_bits);
  REQUIRE(bf.get_num_hashes() == num_hashes);
  REQUIRE(bf.get_bits_used() == 0);
}

TEST_CASE("bloom_filter: inversion", "[bloom_filter]") {
  uint64_t num_bits = 8192;
  uint16_t num_hashes = 3;

  auto bf = bloom_filter_builder::create_by_size(num_bits, num_hashes);

  uint64_t n = 500;
  for (uint64_t i = 0; i < n; ++i) {
    bf.update(i);
  }
  uint64_t num_bits_set = bf.get_bits_used();
  bf.invert();
  REQUIRE(bf.get_bits_used() == num_bits - num_bits_set);

  // original items should be mostly not-present
  uint32_t num_found = 0;
  for (uint64_t i = 0; i < n; ++i) {
    if (bf.query(i)) {
      ++num_found;
    }
  }
  REQUIRE(num_found < n / 10);

  // many other items should be "present"
  num_found = 0;
  for (uint64_t i = n; i < num_bits; ++i) {
    if (bf.query(i)) {
      ++num_found;
    }
  }
  REQUIRE(num_found > n);
}

TEST_CASE("bloom_filter: incompatible set operations", "[bloom_filter]") {
  uint64_t num_bits = 32768;
  uint16_t num_hashes = 4;

  auto bf1 = bloom_filter_builder::create_by_size(num_bits, num_hashes);

  // mismatched num bits
  auto bf2 = bloom_filter_builder::create_by_size(2 * num_bits, num_hashes);
  REQUIRE_THROWS_AS(bf1.union_with(bf2), std::invalid_argument);

  // mismatched num hashes
  auto bf3 = bloom_filter_builder::create_by_size(num_bits, 2 * num_hashes);
  REQUIRE_THROWS_AS(bf1.intersect(bf2), std::invalid_argument);

  // mismatched seed
  auto bf4 = bloom_filter_builder::create_by_size(num_bits, num_hashes, bf1.get_seed() + 1);
  REQUIRE_THROWS_AS(bf1.union_with(bf4), std::invalid_argument);
}

TEST_CASE("bloom_filter: basic union", "[bloom_filter]") {
  const uint64_t num_bits = 12288;
  const uint16_t num_hashes = 4;

  auto bf1 = bloom_filter_builder::create_by_size(num_bits, num_hashes);
  auto bf2 = bloom_filter_builder::create_by_size(num_bits, num_hashes, bf1.get_seed());

  const uint64_t n = 1000;
  const uint32_t max_item = 3 * n / 2 - 1;
  for (uint64_t i = 0; i < n; ++i) {
    bf1.query_and_update(i);
    bf2.update(n / 2 + i);
  }

  bf1.union_with(bf2);
  for (uint64_t i = 0; i < max_item; ++i) {
    REQUIRE(bf1.query(i));
  }

  uint32_t num_found = 0;
  for (uint64_t i = max_item; i < num_bits; ++i) {
    if (bf1.query(i)) {
      ++num_found;
    }
  }
  REQUIRE(num_found < num_bits / 10); // not being super strict
}

TEST_CASE("bloom_filter: basic intersection", "[bloom_filter]") {
  const uint64_t num_bits = 8192;
  const uint16_t num_hahes = 5;

  auto bf1 = bloom_filter_builder::create_by_size(num_bits, num_hahes);
  auto bf2 = bloom_filter_builder::create_by_size(num_bits, num_hahes, bf1.get_seed());

  const uint64_t n = 1024;
  const uint32_t max_item = 3 * n / 2 - 1;
  for (uint64_t i = 0; i < n; ++i) {
    bf1.update(i);
    bf2.update(n / 2 + i);
  }

  bf1.intersect(bf2);
  // overlap bit should all be set
  for (uint64_t i = n / 2; i < n; ++i) {
    REQUIRE(bf1.query(i));
  }

  uint32_t num_found = 0;
  for (uint64_t i = 0; i < n / 2; ++i) {
    if (bf1.query(i)) {
      ++num_found;
    }
  }
  for (uint64_t i = max_item; i < num_bits; ++i) {
    if (bf1.query(i)) {
      ++num_found;
    }
  }

  REQUIRE(num_found < num_bits / 10); // not being super strict
}
/*
TEST_CASE("bloom_filter: empty serialization", "[bloom_filter]") {
  const uint64_t num_bits = 32769;
  const uint16_t num_hashes = 7;

  auto bf = bloom_filter_builder::create_by_size(num_bits, num_hashes);
  auto bytes = bf.serialize();
  REQUIRE(bytes.size() == bf.get_serialized_size_bytes());

  auto bf_bytes = bloom_filter::deserialize(bytes.data(), bytes.size());
  REQUIRE(bf.get_capacity() == bf_bytes.get_capacity());
  REQUIRE(bf.get_seed() == bf_bytes.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_bytes.get_num_hashes());
  REQUIRE(bf_bytes.is_empty());

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  bf.serialize(ss);
  auto bf_stream = bloom_filter::deserialize(ss);
  REQUIRE(bf.get_capacity() == bf_stream.get_capacity());
  REQUIRE(bf.get_seed() == bf_stream.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_stream.get_num_hashes());
  REQUIRE(bf_stream.is_empty());
}
*/
TEST_CASE("bloom_filter: non-empty serialization", "[bloom_filter]") {
  const uint64_t num_bits = 32768;
  const uint16_t num_hashes = 5;

  auto bf = bloom_filter_builder::create_by_size(num_bits, num_hashes);
  const uint64_t n = 1000;
  for (uint64_t i = 0; i < n; ++i) {
    bf.update(0.5 + i); // testing floats
  }

  // test more items without updating, assuming some false positives
  int count = 0;
  for (uint64_t i = n; i < num_bits; ++i) {
    count += bf.query(0.5 + i) ? 1 : 0;
  }

  auto bytes = bf.serialize();
  REQUIRE(bytes.size() == bf.get_serialized_size_bytes());

  auto bf_bytes = bloom_filter::deserialize(bytes.data(), bytes.size());
  REQUIRE(bf.get_capacity() == bf_bytes.get_capacity());
  REQUIRE(bf.get_seed() == bf_bytes.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_bytes.get_num_hashes());
  REQUIRE(!bf_bytes.is_empty());
  int count_bytes = 0;
  for (uint64_t i = 0; i < num_bits; ++i) {
    bool val = bf_bytes.query(0.5 + i);
    if (val) ++count_bytes;
    if (i < n) REQUIRE(val);
  }
  REQUIRE(count_bytes == n + count);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  bf.serialize(ss);
  auto bf_stream = bloom_filter::deserialize(ss);
  REQUIRE(bf.get_capacity() == bf_stream.get_capacity());
  REQUIRE(bf.get_seed() == bf_stream.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_stream.get_num_hashes());
  REQUIRE(!bf_stream.is_empty());
  int count_stream = 0;
  for (uint64_t i = 0; i < num_bits; ++i) {
    bool val = bf_stream.query(0.5 + i);
    if (val) ++count_stream;
    if (i < n) REQUIRE(val);
  }
  REQUIRE(count_stream == n + count);
}

} // namespace datasketches
