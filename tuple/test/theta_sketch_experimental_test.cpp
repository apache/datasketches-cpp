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

#include <iostream>

#include <catch.hpp>
#include <theta_sketch_experimental.hpp>
#include <../../theta/include/theta_sketch.hpp>

namespace datasketches {

TEST_CASE("theta_sketch_experimental: basics ", "[theta_sketch]") {
  auto update_sketch = update_theta_sketch_experimental<>::builder().build();
  update_sketch.update(1);
  update_sketch.update(2);
  REQUIRE(update_sketch.get_num_retained() == 2);
  int count = 0;
  for (const auto& entry: update_sketch) ++count;
  REQUIRE(count == 2);

  auto compact_sketch = update_sketch.compact();
  REQUIRE(compact_sketch.get_num_retained() == 2);
}

//TEST_CASE("theta_sketch_experimental: compare with theta production", "[theta_sketch]") {
//  auto test = theta_sketch_experimental<>::builder().build();
//  update_theta_sketch prod = update_theta_sketch::builder().build();
//
//  for (int i = 0; i < 1000000; ++i) {
//    test.update(i);
//    prod.update(i);
//  }
//
//  std::cout << "--- theta production vs experimental ---" << std::endl;
//  std::cout << test.to_string(true);
//  std::cout << "sizeof(update_theta_sketch)=" << sizeof(update_theta_sketch) << std::endl;
//  std::cout << prod.to_string(true);
//}

} /* namespace datasketches */
