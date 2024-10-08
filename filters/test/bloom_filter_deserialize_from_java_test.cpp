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
#include <fstream>

#include "bloom_filter.hpp"

namespace datasketches {

// assume the binary sketches for this test have been generated by datasketches-java code
// in the subdirectory called "java" in the root directory of this project
static std::string testBinaryInputPath = std::string(TEST_BINARY_INPUT_PATH) + "../../java/";

TEST_CASE("bloom_filter", "[serde_compat]") {
  const uint64_t n_arr[] = {0, 10000, 2000000, 30000000};
  const uint16_t h_arr[] = {3, 5};
  for (const uint64_t n: n_arr) {
    for (const uint16_t num_hashes: h_arr) {
      std::ifstream is;
      is.exceptions(std::ios::failbit | std::ios::badbit);
      is.open(testBinaryInputPath + "bf_n" + std::to_string(n) + "_h" + std::to_string(num_hashes) + "_java.sk", std::ios::binary);
      auto bf = bloom_filter::deserialize(is);
      REQUIRE(bf.is_empty() == (n == 0));
      REQUIRE((bf.is_empty() || (bf.get_bits_used() > n / 10)));

      for (uint64_t i = 0; i < n / 10; ++i) {
        REQUIRE(bf.query(i));
      }
      if (n > 0) REQUIRE(bf.query(std::nan("1")));
    }
  }
}

} /* namespace datasketches */
