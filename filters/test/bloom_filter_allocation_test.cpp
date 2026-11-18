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

#include <sstream>

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

}
