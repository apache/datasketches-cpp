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

#include <iomanip>
#include <iostream>

#include "fast_log2.hpp"
#include "catch2/catch.hpp"

namespace datasketches {
TEST_CASE("fast log2(double) computation", "[fast_log2]") {
  for (int i = 1; i <= 10; i++) {
    const double num = std::pow(2, i);
    REQUIRE(fast_log2_inverse(fast_log2(num) == num));
    REQUIRE(fast_log2(num) == std::log2(num));
    REQUIRE(fast_log2_inverse(i) == num);
  }

  const std::vector<double> nums = {0.5, 0.75, 1.0, 1.5, 3.0, M_PI, M_E, 10.0};
  for (double num : nums) {
    REQUIRE(fast_log2(num) == Approx(std::log2(num)).margin(1e-1));
    REQUIRE(fast_log2_inverse(fast_log2(num) == num));
    REQUIRE(fast_log2_inverse(std::log2(num)) == Approx(num).margin(6e-1));
  }
}
}