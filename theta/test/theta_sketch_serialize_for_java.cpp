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
#include <theta_sketch.hpp>

namespace datasketches {

TEST_CASE("theta sketch generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    auto sketch = update_theta_sketch::builder().build();
    for (unsigned i = 0; i < n; ++i) sketch.update(i);
    REQUIRE(sketch.is_empty() == (n == 0));
    REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.03));
    std::ofstream os("theta_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.compact().serialize(os);
  }
}

TEST_CASE("theta sketch generate compressed", "[serialize_for_java]") {
  const unsigned n_arr[] = {10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    auto sketch = update_theta_sketch::builder().build();
    for (unsigned i = 0; i < n; ++i) sketch.update(i);
    REQUIRE_FALSE(sketch.is_empty());
    REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.03));
    std::ofstream os("theta_compressed_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.compact().serialize_compressed(os);
  }
}

TEST_CASE("theta sketch generate non-empty no entries", "[serialize_for_java]") {
  auto sketch = update_theta_sketch::builder().set_p(0.01).build();
  // here we rely on the fact that hash of 1 happens to be greater than 0.01 (when normalized)
  // and therefore gets rejected
  sketch.update(1);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_num_retained() == 0);
  std::ofstream os("theta_non_empty_no_entries_cpp.sk", std::ios::binary);
  sketch.compact().serialize(os);
}

} /* namespace datasketches */
