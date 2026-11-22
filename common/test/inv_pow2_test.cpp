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

#include <cmath>
#include <cstdint>
#include <cstring>

#include <catch2/catch.hpp>

#include "inv_pow2.hpp"

namespace datasketches {

static uint64_t bits_of(double value) {
  uint64_t bits;
  std::memcpy(&bits, &value, sizeof(bits));
  return bits;
}

TEST_CASE("inv_pow2: exact powers of 2", "[common]") {
  REQUIRE(inv_pow2(0) == 1.0);
  REQUIRE(inv_pow2(1) == 0.5);
  REQUIRE(inv_pow2(64) == 1.0 / 18446744073709551616.0);
  // the smallest normal double
  REQUIRE(inv_pow2(1022) == std::ldexp(1.0, -1022));
  for (uint32_t exp = 0; exp <= 1022; ++exp) {
    CAPTURE(exp);
    REQUIRE(bits_of(inv_pow2(exp)) == bits_of(std::ldexp(1.0, -static_cast<int>(exp))));
    // the table this replaced was filled with pow(2.0, -exp)
    REQUIRE(bits_of(inv_pow2(exp)) == bits_of(std::pow(2.0, -static_cast<double>(exp))));
  }
}

} /* namespace datasketches */
