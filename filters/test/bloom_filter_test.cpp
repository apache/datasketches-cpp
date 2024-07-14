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
//#include <sstream>
//#include <fstream>
//#include <stdexcept>

#include "bloom_filter.hpp"

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

TEST_CASE("bloom_filter: invalid constructor args", "[bloom_filter]") {
  REQUIRE_THROWS_AS(bloom_filter(0, 4, DEFAULT_SEED), std::invalid_argument);
  REQUIRE_THROWS_AS(bloom_filter(1L << 60, 4, DEFAULT_SEED), std::invalid_argument);
  REQUIRE_THROWS_AS(bloom_filter(65535, 0, DEFAULT_SEED), std::invalid_argument);
}

} // namespace datasketches
