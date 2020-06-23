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
#include <tuple_sketch.hpp>
#include <test_allocator.hpp>

namespace datasketches {

using update_tuple_sketch_int_alloc =
    update_tuple_sketch<int, int, default_update_policy<int, int>, serde<int>, test_allocator<int>>;

TEST_CASE("tuple sketch with test allocator: exact mode", "[tuple_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    auto update_sketch = update_tuple_sketch_int_alloc::builder().build();
    for (int i = 0; i < 10000; ++i) update_sketch.update(i, 1);
    for (int i = 0; i < 10000; ++i) update_sketch.update(i, 1);
    REQUIRE(!update_sketch.is_empty());
    REQUIRE(update_sketch.is_estimation_mode());
    unsigned count = 0;
    for (const auto& entry: update_sketch) {
      REQUIRE(entry.second == 2);
      ++count;
    }
    REQUIRE(count == update_sketch.get_num_retained());

    auto compact_sketch = update_sketch.compact();
    REQUIRE(!compact_sketch.is_empty());
    REQUIRE(compact_sketch.is_estimation_mode());
    count = 0;
    for (const auto& entry: compact_sketch) {
      REQUIRE(entry.second == 2);
      ++count;
    }
    REQUIRE(count == update_sketch.get_num_retained());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

} /* namespace datasketches */
