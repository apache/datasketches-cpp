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
#include <tuple_filter.hpp>

namespace datasketches {

TEST_CASE("test", "[tuple_filter]") {
  auto usk = update_tuple_sketch<int>::builder().build();
  tuple_filter<int> f;

  { // empty update sketch
    auto sk = f.compute(usk, [](int){return true;});
    REQUIRE(sk.is_empty());
    REQUIRE(sk.is_ordered());
    REQUIRE(sk.get_num_retained() == 0);
  }

  { // empty compact sketch
    auto sk = f.compute(usk.compact(), [](int){return true;});
    REQUIRE(sk.is_empty());
    REQUIRE(sk.is_ordered());
    REQUIRE(sk.get_num_retained() == 0);
  }

  usk.update(1, 1);
  usk.update(1, 1);
  usk.update(2, 1);
  usk.update(2, 1);
  usk.update(3, 1);

  { // exact mode update sketch
    auto sk = f.compute(usk, [](int v){return v > 1;});
    REQUIRE_FALSE(sk.is_empty());
    REQUIRE_FALSE(sk.is_ordered());
    REQUIRE_FALSE(sk.is_estimation_mode());
    REQUIRE(sk.get_num_retained() == 2);
  }

  { // exact mode compact sketch
    auto sk = f.compute(usk.compact(), [](int v){return v > 1;});
    REQUIRE_FALSE(sk.is_empty());
    REQUIRE(sk.is_ordered());
    REQUIRE_FALSE(sk.is_estimation_mode());
    REQUIRE(sk.get_num_retained() == 2);
  }

  // only keys 1 and 2 had values of 2, which will become 3 after this update
  // some entries are discarded in estimation mode, but these happen to survive
  // the process is deterministic, so the test will always work
  for (int i = 0; i < 10000; ++i) usk.update(i, 1);

  { // estimation mode update sketch
    auto sk = f.compute(usk, [](int v){return v > 2;});
    REQUIRE_FALSE(sk.is_empty());
    REQUIRE_FALSE(sk.is_ordered());
    REQUIRE(sk.is_estimation_mode());
    REQUIRE(sk.get_num_retained() == 2);
  }

  { // estimation mode compact sketch
    auto sk = f.compute(usk.compact(), [](int v){return v > 2;});
    REQUIRE_FALSE(sk.is_empty());
    REQUIRE(sk.is_ordered());
    REQUIRE(sk.is_estimation_mode());
    REQUIRE(sk.get_num_retained() == 2);
  }
}

} /* namespace datasketches */
