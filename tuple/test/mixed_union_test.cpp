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
#include <tuple_union.hpp>
#include <theta_sketch_experimental.hpp>
#include <theta_to_tuple_sketch_adapter.hpp>

namespace datasketches {

TEST_CASE("mixed_union float: empty", "[tuple union]") {
  auto update_sketch = update_theta_sketch_experimental<>::builder().build();

  auto u = tuple_union<float>::builder().build();
  u.update(theta_to_tuple_sketch_adapter<float>(update_sketch, 0));
  auto result = u.get_result();
//  std::cout << result.to_string(true);
  REQUIRE(result.is_empty());
  REQUIRE(result.get_num_retained() == 0);
  REQUIRE(!result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 0);
}

TEST_CASE("mixed_union float: full overlap", "[tuple union]") {
  auto u = tuple_union<float>::builder().build();

  // theta update
  auto update_theta = update_theta_sketch_experimental<>::builder().build();
  for (unsigned i = 0; i < 10; ++i) update_theta.update(i);
  u.update(theta_to_tuple_sketch_adapter<float>(update_theta, 1));

  // theta compact
  auto compact_theta = update_theta.compact();
  u.update(theta_to_tuple_sketch_adapter<float>(compact_theta, 1));

  // tuple update
  auto update_tuple = update_tuple_sketch<float>::builder().build();
  for (unsigned i = 0; i < 10; ++i) update_tuple.update(i, 1);
  u.update(update_tuple);

  // tuple compact
  auto compact_tuple = update_tuple.compact();
  u.update(compact_tuple);

  auto result = u.get_result();
//  std::cout << result.to_string(true);
  REQUIRE_FALSE(result.is_empty());
  REQUIRE(result.get_num_retained() == 10);
  REQUIRE(!result.is_estimation_mode());
  REQUIRE(result.get_estimate() == 10);
  for (const auto& entry: result) {
    REQUIRE(entry.second == 4);
  }
}

} /* namespace datasketches */
