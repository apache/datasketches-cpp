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
  REQUIRE_THROWS_AS(bloom_filter::builder::create_by_size(0, 4), std::invalid_argument);
  REQUIRE_THROWS_AS(bloom_filter::builder::create_by_size(1L << 60, 4), std::invalid_argument);
  REQUIRE_THROWS_AS(bloom_filter::builder::create_by_size(65535, 0), std::invalid_argument);
}

TEST_CASE("bloom_filter: standard constructors", "[bloom_filter]") {
  uint64_t num_items = 4000;
  double fpp = 0.01;

  uint64_t num_bits = bloom_filter::builder::suggest_num_filter_bits(num_items, fpp);
  uint16_t num_hashes = bloom_filter::builder::suggest_num_hashes(num_items, num_bits);
  uint64_t seed = 89023;

  auto bf = bloom_filter::builder::create_by_size(num_bits, num_hashes, seed);
  uint64_t adjusted_num_bits = (num_bits + 63) & ~0x3F; // round up to the nearest multiple of 64
  REQUIRE(bf.get_capacity() == adjusted_num_bits);
  REQUIRE(bf.get_num_hashes() == num_hashes);
  REQUIRE(bf.get_seed() == seed);
  REQUIRE(bf.is_empty());

  // should match above
  bf = bloom_filter::builder::create_by_accuracy(num_items, fpp, seed);
  REQUIRE(bf.get_capacity() == adjusted_num_bits);
  REQUIRE(bf.get_num_hashes() == num_hashes);
  REQUIRE(bf.get_seed() == seed);
  REQUIRE(bf.is_empty());

  // same for initializing memory in-place
  size_t serialized_size_bytes = bloom_filter::get_serialized_size_bytes(num_bits);
  uint8_t* bytes = new uint8_t[serialized_size_bytes];

  bf = bloom_filter::builder::initialize_by_size(bytes, serialized_size_bytes, num_bits, num_hashes, seed);
  REQUIRE(bf.get_capacity() == adjusted_num_bits);
  REQUIRE(bf.get_num_hashes() == num_hashes);
  REQUIRE(bf.get_seed() == seed);
  REQUIRE(bf.is_empty());

  bf = bloom_filter::builder::initialize_by_accuracy(bytes, serialized_size_bytes, num_items, fpp, seed);
  REQUIRE(bf.get_capacity() == adjusted_num_bits);
  REQUIRE(bf.get_num_hashes() == num_hashes);
  REQUIRE(bf.get_seed() == seed);
  REQUIRE(bf.is_empty());

  delete [] bytes;
}

TEST_CASE("bloom_filter: basic operations", "[bloom_filter]") {
  uint64_t num_items = 5000;
  double fpp = 0.01;

  auto bf = bloom_filter::builder::create_by_accuracy(num_items, fpp);
  REQUIRE(bf.is_empty());
  REQUIRE(bf.get_bits_used() == 0);

  for (uint64_t i = 0; i < num_items; ++i) {
    bf.query_and_update(i);
  }

  REQUIRE(!bf.is_empty());
  // filter is about 50% full at target capacity
  REQUIRE(bf.get_bits_used() == Approx(0.5 * bf.get_capacity()).epsilon(0.05));

  uint32_t num_found = 0;
  for (uint64_t i = num_items; i < bf.get_capacity(); ++i) {
    if (bf.query(i)) {
      ++num_found;
    }
  }
  // fpp is average with significant variance
  REQUIRE(num_found == Approx((bf.get_capacity() - num_items) * fpp).epsilon(0.12));
  auto bytes = bf.serialize();

  // initialize in memory and run the same tests
  // also checking against the results from the first part
  uint8_t* bf_memory = new uint8_t[bytes.size()];
  auto bf2 = bloom_filter::builder::initialize_by_accuracy(bf_memory, bytes.size(), num_items, fpp, bf.get_seed());
  REQUIRE(bf2.is_empty());
  REQUIRE(bf2.get_bits_used() == 0);

  for (uint64_t i = 0; i < num_items; ++i) {
    bf2.query_and_update(i);
  }

  REQUIRE(!bf2.is_empty());
  REQUIRE(bf2.get_bits_used() == bf.get_bits_used()); // should exactly match above

  uint32_t num_found2 = 0;
  for (uint64_t i = num_items; i < bf2.get_capacity(); ++i) {
    if (bf2.query(i)) {
      ++num_found2;
    }
  }
  REQUIRE(num_found == num_found2); // should exactly match above
  auto bytes2 = bf2.serialize();

  REQUIRE(bytes.size() == bytes2.size());
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(bytes[i] == bytes2[i]);
  }

  // check that raw memory also matches serialized sketch
  const uint8_t* bf_bytes = bf2.get_wrapped_memory();
  REQUIRE(bf_bytes == bf_memory);
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(bf_bytes[i] == bytes[i]);
  }

  // ensure the filters reset properly
  bf.reset();
  REQUIRE(bf.is_empty());
  REQUIRE(bf.get_bits_used() == 0);

  bf2.reset();
  REQUIRE(bf2.is_empty());
  REQUIRE(bf2.get_bits_used() == 0);

  delete [] bf_memory;
}

TEST_CASE("bloom_filter: inversion", "[bloom_filter]") {
  uint64_t num_bits = 8192;
  uint16_t num_hashes = 3;

  auto bf = bloom_filter::builder::create_by_size(num_bits, num_hashes);

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

  auto bf1 = bloom_filter::builder::create_by_size(num_bits, num_hashes);

  // mismatched num bits
  auto bf2 = bloom_filter::builder::create_by_size(2 * num_bits, num_hashes);
  REQUIRE_THROWS_AS(bf1.union_with(bf2), std::invalid_argument);

  // mismatched num hashes
  auto bf3 = bloom_filter::builder::create_by_size(num_bits, 2 * num_hashes);
  REQUIRE_THROWS_AS(bf1.intersect(bf2), std::invalid_argument);

  // mismatched seed
  auto bf4 = bloom_filter::builder::create_by_size(num_bits, num_hashes, bf1.get_seed() + 1);
  REQUIRE_THROWS_AS(bf1.union_with(bf4), std::invalid_argument);
}

TEST_CASE("bloom_filter: basic union", "[bloom_filter]") {
  const uint64_t num_bits = 12288;
  const uint16_t num_hashes = 4;

  auto bf1 = bloom_filter::builder::create_by_size(num_bits, num_hashes);
  auto bf2 = bloom_filter::builder::create_by_size(num_bits, num_hashes, bf1.get_seed());

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

  auto bf1 = bloom_filter::builder::create_by_size(num_bits, num_hahes);
  auto bf2 = bloom_filter::builder::create_by_size(num_bits, num_hahes, bf1.get_seed());

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

TEST_CASE("bloom_filter: empty serialization", "[bloom_filter]") {
  const uint64_t num_bits = 32769;
  const uint16_t num_hashes = 7;

  auto bf = bloom_filter::builder::create_by_size(num_bits, num_hashes);
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

  // read-only wrap should work
  auto bf_wrap = bloom_filter::wrap(bytes.data(), bytes.size());
  REQUIRE(bf.get_capacity() == bf_wrap.get_capacity());
  REQUIRE(bf.get_seed() == bf_wrap.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_wrap.get_num_hashes());
  REQUIRE(bf_wrap.is_empty());

  // writable wrap should not
  REQUIRE_THROWS_AS(bloom_filter::writable_wrap(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("bloom_filter: non-empty serialization", "[bloom_filter]") {
  const uint64_t num_bits = 32768;
  const uint16_t num_hashes = 5;

  auto bf = bloom_filter::builder::create_by_size(num_bits, num_hashes);
  const uint64_t n = 1000;
  for (uint64_t i = 0; i < n; ++i) {
    bf.update(0.5 + i); // testing floats
  }

  // test more items without updating, assuming some false positives
  // so we can check that we get the same number of false positives
  // with the same query items
  uint64_t fp_count = 0;
  for (uint64_t i = n; i < num_bits; ++i) {
    fp_count += bf.query(0.5 + i) ? 1 : 0;
  }

  auto bytes = bf.serialize();
  REQUIRE(bytes.size() == bf.get_serialized_size_bytes());

  auto bf_bytes = bloom_filter::deserialize(bytes.data(), bytes.size());
  REQUIRE(bf.get_capacity() == bf_bytes.get_capacity());
  REQUIRE(bf.get_seed() == bf_bytes.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_bytes.get_num_hashes());
  REQUIRE(!bf_bytes.is_empty());
  REQUIRE(bf.is_memory_owned());
  uint64_t fp_count_bytes = 0;
  for (uint64_t i = 0; i < num_bits; ++i) {
    bool val = bf_bytes.query(0.5 + i);
    if (i < n)
      REQUIRE(val);
    else if (val)
      ++fp_count_bytes;
  }
  REQUIRE(fp_count_bytes == fp_count);

  std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
  bf.serialize(ss);
  auto bf_stream = bloom_filter::deserialize(ss);
  REQUIRE(bf.get_capacity() == bf_stream.get_capacity());
  REQUIRE(bf.get_seed() == bf_stream.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_stream.get_num_hashes());
  REQUIRE(!bf_stream.is_empty());
  REQUIRE(bf_stream.is_memory_owned());
  uint64_t fp_count_stream = 0;
  for (uint64_t i = 0; i < num_bits; ++i) {
    bool val = bf_stream.query(0.5 + i);
    if (i < n)
      REQUIRE(val);
    else if (val)
      ++fp_count_stream;
  }
  REQUIRE(fp_count_stream == fp_count);

  // read-only wrap
  auto bf_wrap = bloom_filter::wrap(bytes.data(), bytes.size());
  REQUIRE(bf.get_capacity() == bf_wrap.get_capacity());
  REQUIRE(bf.get_seed() == bf_wrap.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_wrap.get_num_hashes());
  REQUIRE(!bf_wrap.is_empty());
  REQUIRE(!bf_wrap.is_memory_owned());
  uint64_t fp_count_wrap = 0;
  for (uint64_t i = 0; i < num_bits; ++i) {
    bool val = bf_wrap.query(0.5 + i);
    if (i < n)
      REQUIRE(val);
    else if (val)
      ++fp_count_wrap;
  }
  REQUIRE(fp_count_wrap == fp_count);
  REQUIRE_THROWS_AS(bf_wrap.update(-1.0), std::logic_error);
  REQUIRE_THROWS_AS(bf_wrap.query_and_update(-2.0), std::logic_error);
  REQUIRE_THROWS_AS(bf_wrap.reset(), std::logic_error);

  // writable wrap
  auto bf_writable = bloom_filter::writable_wrap(bytes.data(), bytes.size());
  REQUIRE(bf.get_capacity() == bf_writable.get_capacity());
  REQUIRE(bf.get_seed() == bf_writable.get_seed());
  REQUIRE(bf.get_num_hashes() == bf_writable.get_num_hashes());
  REQUIRE(!bf_writable.is_empty());
  REQUIRE(!bf_writable.is_memory_owned());
  uint64_t fp_count_writable = 0;
  for (uint64_t i = 0; i < num_bits; ++i) {
    bool val = bf_writable.query(0.5 + i);
    if (i < n)
      REQUIRE(val);
    else if (val)
      ++fp_count_writable;
  }
  REQUIRE(fp_count_writable == fp_count);

  REQUIRE(!bf_writable.query(-1.0));
  bf_writable.update(-1.0);
  REQUIRE(bf_writable.query(-1.0));

  // not good memory management to do this, but because we wrapped the same bytes as both
  // read-only adn writable, that update should ahve changed the read-only version, too
  REQUIRE(bf_wrap.query(-1.0));
}

} // namespace datasketches
