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

#include <ebpps_sample.hpp>

#include <catch2/catch.hpp>

#include <vector>
#include <string>
#include <sstream>
#include <fstream>
#include <cmath>
#include <random>
#include <stdexcept>

// TODO: remove when done testing
#include <var_opt_sketch.hpp>
#include <var_opt_union.hpp>
#include <iomanip>
#include <algorithm>

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

static constexpr double EPS = 1e-13;

static ebpps_sketch<int> create_unweighted_sketch(uint32_t k, uint64_t n) {
  ebpps_sketch<int> sk(k);
  for (uint64_t i = 0; i < n; ++i) {
    sk.update(static_cast<int>(i), 1.0);
  }
  return sk;
}

template<typename T, typename A>
static void check_if_equal(ebpps_sketch<T, A>& sk1, ebpps_sketch<T, A>& sk2) {
  REQUIRE(sk1.get_k() == sk2.get_k());
  REQUIRE(sk1.get_n() == sk2.get_n());
  REQUIRE(sk1.get_num_samples() == sk2.get_num_samples());

  auto it1 = sk1.begin();
  auto it2 = sk2.begin();

  while ((it1 != sk1.end()) && (it2 != sk2.end())) {
    auto p1 = *it1;
    auto p2 = *it2;
    REQUIRE(p1.first == p2.first);   // data values
    REQUIRE(p1.second == p2.second); // weights
    ++it1;
    ++it2;
  }

  REQUIRE((it1 == sk1.end() && it2 == sk2.end())); // iterators must end at the same time
}

TEST_CASE("ebpps sketch: invalid k", "[ebpps_sketch]") {
  REQUIRE_THROWS_AS(ebpps_sketch<int>(0), std::invalid_argument);
  REQUIRE_THROWS_AS(ebpps_sketch<int>(ebpps_constants::MAX_K + 1), std::invalid_argument);
}
