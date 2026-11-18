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

#include <cstring>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <catch2/catch.hpp>

#include "bloom_filter.hpp"
#include "test_type.hpp"
#include "test_allocator.hpp"

namespace datasketches {

using bloom_filter_test_alloc = bloom_filter_alloc<test_allocator<test_type>>;
using alloc = test_allocator<test_type>;

TEST_CASE("bloom filter allocation test", "[bloom_filter][test_type]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    int64_t num_items = 10000;
    double fpp = 0.01;
    uint64_t seed = bloom_filter_test_alloc::builder::generate_random_seed();
    auto bf1 = bloom_filter_test_alloc::builder::create_by_accuracy(num_items,
                                                                   fpp,
                                                                   seed,
                                                                   alloc(0));
    for (int i = 0; i < num_items; ++i) {
      if (num_items % 1 == 0) {
        bf1.update(std::to_string(i));
      } else {
        bf1.update(i);
      }
    }
    auto bytes1 = bf1.serialize(0);
    auto bf2 = bloom_filter_test_alloc::deserialize(bytes1.data(), bytes1.size(), 0);

    std::stringstream ss;
    bf1.serialize(ss);
    auto bf3 = bloom_filter_test_alloc::deserialize(ss, alloc(0));

    bf3.reset();
    for (int i = 0; i < num_items; ++i) {
      bf1.update(-1.0 * i);
    }

    bf3.union_with(bf1);

    auto bytes2 = bf3.serialize(0);
    auto bf4 = bloom_filter_test_alloc::deserialize(bytes2.data(), bytes2.size(), 0);

    auto bf5 = bloom_filter_test_alloc::wrap(bytes2.data(), bytes2.size(), 0);
    auto bf6 = bloom_filter_test_alloc::writable_wrap(bytes2.data(), bytes2.size(), 0);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("bloom filter: truncated stream frees bit array", "[bloom_filter][test_type]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    auto bf = bloom_filter_test_alloc::builder::create_by_accuracy(1000, 0.01, 12345, alloc(0));
    for (int i = 0; i < 100; ++i) bf.update(i);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    bf.serialize(ss);
    std::string serialized = ss.str();
    serialized.resize(serialized.size() - 8);

    // bf is dirty, so num_bits_set validation is skipped and only the stream check catches truncation
    std::istringstream is(serialized, std::ios::in | std::ios::binary);
    REQUIRE_THROWS_AS(bloom_filter_test_alloc::deserialize(is, alloc(0)), std::runtime_error);

    std::istringstream is_throwing(serialized, std::ios::in | std::ios::binary);
    is_throwing.exceptions(std::ios::failbit | std::ios::badbit);
    REQUIRE_THROWS_AS(bloom_filter_test_alloc::deserialize(is_throwing, alloc(0)), std::ios_base::failure);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

namespace {

// The size in bytes that throwing_allocator was last asked for.
std::size_t last_requested_bytes = 0;

// Standard-conforming allocator that always fails: allocate() throws std::bad_alloc.
// It records the requested size first, so that tests can check it without allocating it.
template<typename T>
struct throwing_allocator {
  using value_type = T;
  throwing_allocator() = default;
  template<typename U> throwing_allocator(const throwing_allocator<U>&) {}
  T* allocate(std::size_t n) {
    last_requested_bytes = n * sizeof(T);
    throw std::bad_alloc();
  }
  void deallocate(T*, std::size_t) {}
  bool operator==(const throwing_allocator&) const { return true; }
  bool operator!=(const throwing_allocator&) const { return false; }
};

// A bloom filter whose allocator always throws std::bad_alloc.
using throwing_filter = bloom_filter_alloc<throwing_allocator<uint8_t>>;

// A serialized non-empty filter cut down to its 32-byte header, whose header says that its
// bit array has num_longs longs. Its count of bits set is 0, consistent with having no bits.
std::vector<uint8_t> header_only_image(uint32_t num_longs) {
  const size_t num_longs_offset = 16; // offsets in the serialized image, see serialize()
  const size_t num_bits_set_offset = 24;
  const size_t header_bytes = 32;
  const uint64_t num_bits_set = 0;

  auto bf = bloom_filter::builder::create_by_size(64, 3, 1234);
  bf.update(1);
  auto image = bf.serialize();
  image.resize(header_bytes);
  std::memcpy(image.data() + num_longs_offset, &num_longs, sizeof(num_longs));
  std::memcpy(image.data() + num_bits_set_offset, &num_bits_set, sizeof(num_bits_set));
  return image;
}

} // namespace

TEST_CASE("bloom filter: allocation failure propagates std::bad_alloc", "[bloom_filter]") {
  const throwing_allocator<uint8_t> allocator;
  const uint64_t num_bits = 1024;
  const uint16_t num_hashes = 3;
  const uint64_t seed = 1234;

  REQUIRE_THROWS_AS(throwing_filter::builder::create_by_size(num_bits, num_hashes, seed, allocator),
                    std::bad_alloc);

  auto bf = bloom_filter::builder::create_by_size(num_bits, num_hashes, seed);
  bf.update(1);
  auto bytes = bf.serialize();
  REQUIRE_THROWS_AS(throwing_filter::deserialize(bytes.data(), bytes.size(), allocator),
                    std::bad_alloc);
  std::stringstream ss;
  bf.serialize(ss);
  REQUIRE_THROWS_AS(throwing_filter::deserialize(ss, allocator), std::bad_alloc);
}

TEST_CASE("bloom filter: the size of the bit array is computed in 64 bits", "[bloom_filter]") {
  // The header of an image says how many 64-bit longs its bit array has. 2^29 longs is 2^32 bytes.
  // Computed in 32 bits, that size would be 0, and deserialize() would call allocate(0).
  const uint32_t num_longs = uint32_t(1) << 29;
  const uint64_t expected_num_bytes = uint64_t(num_longs) << 3;

  // Precondition: computed in 32 bits, the size of this bit array really overflows to 0.
  const uint32_t num_bytes_in_32_bits = num_longs << 3;
  REQUIRE(num_bytes_in_32_bits == 0);

  // An image with this header and no bit array at all.
  const auto image = header_only_image(num_longs);

  SECTION("from memory: rejected, because the image is far too short for 2^32 bytes") {
    // Before it allocates anything, deserialize() checks that the buffer holds num_bytes:
    // ensure_minimum_memory() throws std::out_of_range. With a size of 0, that check passed.
    REQUIRE_THROWS_AS(bloom_filter::deserialize(image.data(), image.size()), std::out_of_range);
  }

  SECTION("from a stream: the allocator is asked for 2^32 bytes, not 0") {
    // A stream can't tell how much data follows, so deserialize() allocates first.
    // throwing_allocator records the size it was asked for, then refuses: nothing is allocated.
    const throwing_allocator<uint8_t> allocator;
    const std::string image_string(reinterpret_cast<const char*>(image.data()), image.size());
    std::istringstream image_stream(image_string, std::ios::in | std::ios::binary);
    last_requested_bytes = 0;

    // throwing_allocator refuses every request, so deserialize() throws std::bad_alloc here
    // in any case, also on the old code. This only confirms that the request reached the allocator.
    REQUIRE_THROWS_AS(throwing_filter::deserialize(image_stream, allocator), std::bad_alloc);

    // What this section checks: the size of that request.
    const std::size_t requested_bytes = last_requested_bytes;
    REQUIRE(requested_bytes != 0);                    // a request for 0 bytes is the 32-bit overflow
    REQUIRE(requested_bytes == expected_num_bytes);   // the whole 2^32 bytes
  }
}

}
