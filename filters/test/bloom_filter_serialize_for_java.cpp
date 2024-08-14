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
#include <fstream>

#include "bloom_filter.hpp"

namespace datasketches {

TEST_CASE("bloom filter generate", "[serialize_for_java]") {
  const uint64_t n_arr[] = {0, 10000, 2000000, 30000000};
  const double h_arr[] = {3, 5};
  for (const uint64_t n: n_arr) {
    for (const uint16_t num_hashes: h_arr) {
      const uint64_t config_bits = std::max(n, static_cast<uint64_t>(1000)); // so empty still has valid bit size
      bloom_filter bf = bloom_filter_builder::create_by_size(config_bits, num_hashes);
      for (uint64_t i = 0; i < n / 10; ++i) bf.update(i); // note: n / 10 items into n bits
      if (n > 0) bf.update(std::nan("1")); // include a NaN if non-empty
      REQUIRE(bf.is_empty() == (n == 0));
      REQUIRE((bf.is_empty() || (bf.get_bits_used() > n / 10)));
      std::ofstream os("bf_n" + std::to_string(n) + "_h" + std::to_string(num_hashes) + "_cpp.sk", std::ios::binary);
      bf.serialize(os);
    }
  }
}

} /* namespace datasketches */
