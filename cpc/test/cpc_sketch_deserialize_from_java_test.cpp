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
#include <cpc_sketch.hpp>

namespace datasketches {

static std::string testBinaryInputPath = std::string(TEST_BINARY_INPUT_PATH) + "../../java/";

TEST_CASE("cpc sketch", "[serde_compat]") {
  unsigned n_arr[] = {0, 100, 200, 2000, 20000};
  for (unsigned n: n_arr) {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "cpc_n" + std::to_string(n) + ".sk", std::ios::binary);
    auto sketch = cpc_sketch::deserialize(is);
    REQUIRE(sketch.is_empty() == (n == 0));
    REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.02));
  }
}

} /* namespace datasketches */
