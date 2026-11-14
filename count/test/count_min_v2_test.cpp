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

// Tests of serialization version 2, against the Count-Min format specification in datasketches-tck
// (docs/count-min-format.md). Section numbers refer to that specification.

#include <catch2/catch.hpp>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "count_min.hpp"
#include "xxhash64.h"

namespace datasketches {

namespace {

using sketch_type = count_min_sketch<int64_t>;

// indices of the non-zero counters
std::vector<size_t> nonzero_counters(const sketch_type& sketch) {
  std::vector<size_t> indices;
  size_t index = 0;
  for (auto it = sketch.begin(); it != sketch.end(); ++it, ++index) {
    if (*it != 0) indices.push_back(index);
  }
  return indices;
}

template<typename Item>
std::vector<size_t> counters_of(uint64_t seed, uint32_t num_buckets, const Item& item) {
  sketch_type sketch(3, num_buckets, seed);
  sketch.update(item, 1);
  return nonzero_counters(sketch);
}

std::vector<size_t> rows(size_t b0, size_t b1, size_t b2, size_t num_buckets = 1000) {
  return {b0, num_buckets + b1, 2 * num_buckets + b2};
}

std::vector<uint8_t> to_vector(const sketch_type::vector_bytes& bytes) {
  return std::vector<uint8_t>(bytes.begin(), bytes.end());
}

} // namespace

TEST_CASE("count min v2: XXH64 test vectors (7.1)", "[count_min]") {
  REQUIRE(XXHash64::hash("", 0, 0) == 0xEF46DB3751D8E999ULL);
  REQUIRE(XXHash64::hash("a", 1, 0) == 0xD24EC4F1A98C6E5BULL);
  REQUIRE(XXHash64::hash("abc", 3, 0) == 0x44BC2CF5AD770999ULL);
}

TEST_CASE("count min v2: row seed test vectors (7.2)", "[count_min]") {
  // rowSeed[i] = XXH64(LE64(i), seed = baseSeed)
  auto row_seed = [](uint64_t i, uint64_t base_seed) { return XXHash64::hash(&i, sizeof(i), base_seed); };
  REQUIRE(row_seed(0, 9001) == 0xD71D64972584A288ULL);
  REQUIRE(row_seed(1, 9001) == 0xEFE8454A6499F311ULL);
  REQUIRE(row_seed(2, 9001) == 0x23819BE21B1B1D9EULL);
  REQUIRE(row_seed(0, 42) == 0xB71B47EBDA15746CULL);
  REQUIRE(row_seed(1, 42) == 0x9ED50FD59358D232ULL);
  REQUIRE(row_seed(2, 42) == 0xD19FE30002B9A3CDULL);
}

TEST_CASE("count min v2: bucket test vectors (7.3)", "[count_min]") {
  REQUIRE(counters_of(9001, 1000, int64_t(1)) == rows(185, 898, 4));
  REQUIRE(counters_of(9001, 1000, uint64_t(1)) == rows(185, 898, 4)); // the same 8 bytes
  REQUIRE(counters_of(9001, 1000, int64_t(9)) == rows(490, 217, 440));
  REQUIRE(counters_of(9001, 1000, int64_t(-1)) == rows(256, 641, 308));
  REQUIRE(counters_of(9001, 1000, uint64_t(0x8000000000000000ULL)) == rows(951, 0, 298));
  REQUIRE(counters_of(9001, 1000, std::string("apple")) == rows(130, 747, 144));
  REQUIRE(counters_of(9001, 1000, std::string("\xc3\xa9t\xc3\xa9")) == rows(806, 491, 163)); // "été" in UTF-8
  REQUIRE(counters_of(42, 1000, int64_t(1)) == rows(468, 459, 696));
  REQUIRE(counters_of(42, 1000, int64_t(9)) == rows(841, 428, 322));
}

TEST_CASE("count min v2: image test vectors (7.4)", "[count_min]") {
  sketch_type sketch(2, 9, 9001);
  REQUIRE(sketch.get_serial_version() == 2);
  const std::vector<uint8_t> empty_image = {
    0x02, 0x02, 0x12, 0x01, 0x00, 0x00, 0x00, 0x00,  0x09, 0x00, 0x00, 0x00, 0x02, 0xcc, 0x93, 0x00
  };
  REQUIRE(to_vector(sketch.serialize()) == empty_image);

  sketch.update(int64_t(1), 10);
  sketch.update(int64_t(9), 810);
  sketch.update(std::string("apple"), 5);
  std::vector<uint8_t> image(168, 0);
  const uint8_t header[] = {
    0x02, 0x02, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00,  0x09, 0x00, 0x00, 0x00, 0x02, 0xcc, 0x93, 0x00
  };
  std::memcpy(image.data(), header, sizeof(header));
  auto put = [&image](size_t offset, int64_t value) { std::memcpy(image.data() + offset, &value, sizeof(value)); };
  put(16, 825);  // total weight
  put(24, 810);  // row 0, bucket 0: 9
  put(56, 5);    // row 0, bucket 4: "apple"
  put(72, 10);   // row 0, bucket 6: 1
  put(104, 5);   // row 1, bucket 1: "apple"
  put(112, 810); // row 1, bucket 2: 9
  put(152, 10);  // row 1, bucket 7: 1
  const auto bytes = sketch.serialize();
  REQUIRE(to_vector(bytes) == image);

  const auto read = sketch_type::deserialize(bytes.data(), bytes.size(), 9001);
  REQUIRE(read.get_serial_version() == 2);
  REQUIRE(read.get_estimate(int64_t(1)) == 10);
  REQUIRE(read.get_estimate(int64_t(9)) == 810);
  REQUIRE(read.get_estimate(std::string("apple")) == 5);
}

// 4.4: an empty string, an empty byte array and a null are ignored, and every query returns 0
TEST_CASE("count min v2: empty items are ignored (4.4)", "[count_min]") {
  sketch_type sketch(3, 1000);
  sketch.update(nullptr, 0, 7);
  sketch.update("x", 0, 7);
  sketch.update(std::string(), 7);
  REQUIRE(sketch.is_empty());
  REQUIRE(sketch.get_total_weight() == 0);

  // on a non-empty sketch, so that a missing check on the upper bound would show
  sketch.update(int64_t(1), 1000);
  const auto bytes = to_vector(sketch.serialize());
  sketch.update(nullptr, 0, 7);
  sketch.update("x", 0, 7);
  sketch.update(std::string(), 7);
  REQUIRE(to_vector(sketch.serialize()) == bytes);
  REQUIRE(sketch.get_estimate(nullptr, 0) == 0);
  REQUIRE(sketch.get_lower_bound(nullptr, 0) == 0);
  REQUIRE(sketch.get_upper_bound(nullptr, 0) == 0);
  REQUIRE(sketch.get_estimate("x", 0) == 0);
  REQUIRE(sketch.get_upper_bound("x", 0) == 0);
  REQUIRE(sketch.get_estimate(std::string()) == 0);
  REQUIRE(sketch.get_lower_bound(std::string()) == 0);
  REQUIRE(sketch.get_upper_bound(std::string()) == 0);
  REQUIRE(sketch.get_upper_bound(int64_t(2)) > 0); // a non-empty item that was never added still gets eps * weight
}

// 1: numHashes is 1 to 127
TEST_CASE("count min v2: number of hashes", "[count_min]") {
  REQUIRE_THROWS_AS(sketch_type(0, 1000), std::invalid_argument);
  REQUIRE_THROWS_AS(sketch_type(128, 1000), std::invalid_argument);
  REQUIRE_THROWS_AS(sketch_type(255, 1000), std::invalid_argument);
  REQUIRE(sketch_type(1, 1000).get_num_hashes() == 1);
  REQUIRE(sketch_type(127, 1000).get_num_hashes() == 127);
  REQUIRE(sketch_type::suggest_num_hashes(1.0) == 127);
  REQUIRE(sketch_type::suggest_num_hashes(0.0) == 1);

  // the same limits apply to version 2 images
  auto image = to_vector(sketch_type(3, 1000).serialize());
  image[12] = 0;
  REQUIRE_THROWS_AS(sketch_type::deserialize(image.data(), image.size()), std::invalid_argument);
  image[12] = 128;
  REQUIRE_THROWS_AS(sketch_type::deserialize(image.data(), image.size()), std::invalid_argument);
}

// 6: readers accept version 1 images, keep their hashing and write them back as version 1
TEST_CASE("count min v2: version 1 images", "[count_min]") {
  // written by the C++ library before version 2: numHashes 2, numBuckets 5, seed 9001,
  // update(1, 10), update(9, 810), update("apple", 5)
  const std::vector<uint8_t> v1_image = {
    0x02, 0x01, 0x12, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x05, 0x00, 0x00, 0x00, 0x02, 0xcc, 0x93, 0x00,
    0x39, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x2a, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x34, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };
  auto v1 = sketch_type::deserialize(v1_image.data(), v1_image.size(), 9001);
  REQUIRE(v1.get_serial_version() == 1);
  REQUIRE(v1.get_total_weight() == 825);
  REQUIRE(to_vector(v1.serialize()) == v1_image); // written back as version 1

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  s.write(reinterpret_cast<const char*>(v1_image.data()), v1_image.size());
  REQUIRE(sketch_type::deserialize(s, 9001).get_serial_version() == 1);

  // Version 1 row seeds depend on the standard library, so the estimates of the items above
  // are checked only with the standard library that wrote the image. A version 1 sketch is
  // consistent with itself everywhere.
  v1.update(std::string("pear"), 3);
  REQUIRE(v1.get_estimate(std::string("pear")) >= 3);
  REQUIRE(v1.get_total_weight() == 828);
  const auto v1_bytes = v1.serialize();
  REQUIRE(v1_bytes[1] == 1);

  // version 1 and version 2 sketches hash differently, so they can't be merged
  sketch_type v2(2, 5, 9001);
  v2.update(int64_t(1), 1);
  REQUIRE_THROWS_AS(v1.merge(v2), std::invalid_argument);
  REQUIRE_THROWS_AS(v2.merge(v1), std::invalid_argument);
  auto other_v1 = sketch_type::deserialize(v1_image.data(), v1_image.size(), 9001);
  v1.merge(other_v1);
  REQUIRE(v1.get_total_weight() == 828 + 825);
  REQUIRE(v1.get_serial_version() == 1);

  // version 1 images written by C++ may have up to 255 rows; 0 is never valid
  std::vector<uint8_t> v1_empty = {
    0x02, 0x01, 0x12, 0x01, 0x00, 0x00, 0x00, 0x00,  0x03, 0x00, 0x00, 0x00, 200, 0xcc, 0x93, 0x00
  };
  const auto wide = sketch_type::deserialize(v1_empty.data(), v1_empty.size(), 9001);
  REQUIRE(wide.get_serial_version() == 1);
  REQUIRE(wide.get_num_hashes() == 200);
  v1_empty[12] = 0;
  REQUIRE_THROWS_AS(sketch_type::deserialize(v1_empty.data(), v1_empty.size(), 9001), std::invalid_argument);
}

TEST_CASE("count min v2: other serial versions are rejected", "[count_min]") {
  for (uint8_t version: {0, 3, 255}) {
    auto image = to_vector(sketch_type(3, 1000).serialize());
    image[1] = version;
    REQUIRE_THROWS_AS(sketch_type::deserialize(image.data(), image.size()), std::invalid_argument);
  }
}

TEST_CASE("count min v2: copies keep the serial version", "[count_min]") {
  const std::vector<uint8_t> v1_empty = {
    0x02, 0x01, 0x12, 0x01, 0x00, 0x00, 0x00, 0x00,  0x09, 0x00, 0x00, 0x00, 0x02, 0xcc, 0x93, 0x00
  };
  const auto v1 = sketch_type::deserialize(v1_empty.data(), v1_empty.size());
  sketch_type copy(v1);
  REQUIRE(copy.get_serial_version() == 1);
  sketch_type assigned(3, 10);
  assigned = v1;
  REQUIRE(assigned.get_serial_version() == 1);
  sketch_type moved(std::move(copy));
  REQUIRE(moved.get_serial_version() == 1);
  REQUIRE(moved.serialize()[1] == 1);
}

} /* namespace datasketches */
