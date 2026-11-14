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

#include "tdigest.hpp"
#include "test_allocator.hpp"

namespace datasketches {

using alloc_d = test_allocator<double>;
using tdigest_d = tdigest<double, alloc_d>;

TEST_CASE("tdigest custom allocator", "[tdigest]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    tdigest_d td(100, alloc_d(0));
    for (int i = 0; i < 10000; ++i) td.update(static_cast<double>(i));
    REQUIRE(test_allocator_total_bytes != 0);
    REQUIRE(test_allocator_net_allocations != 0);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("tdigest custom allocator: buffers grow as they fill", "[tdigest]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    tdigest_d empty(100, alloc_d(0));
    REQUIRE(test_allocator_total_bytes == 0); // nothing reserved up front

    tdigest_d small(100, alloc_d(0));
    for (int i = 0; i < 3; ++i) small.update(static_cast<double>(i));
    // a few values take a small buffer, not room for 4 * (2k + fudge) values
    REQUIRE(test_allocator_total_bytes <= static_cast<long long>(8 * sizeof(double)));
    REQUIRE(small.get_total_weight() == 3);
    REQUIRE(small.get_quantile(0.5) == 1.0);

    // a digest built through the growing buffers gives the same result as before
    tdigest_d grown(100, alloc_d(0));
    tdigest<double> reference(100);
    for (int i = 0; i < 100000; ++i) {
      grown.update(static_cast<double>(i % 7919));
      reference.update(static_cast<double>(i % 7919));
    }
    for (double rank: {0.01, 0.25, 0.5, 0.75, 0.99}) {
      REQUIRE(grown.get_quantile(rank) == reference.get_quantile(rank));
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

} /* namespace datasketches */
