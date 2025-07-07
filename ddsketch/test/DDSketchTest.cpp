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

#include <catch2/catch.hpp>

#include "collapsing_highest_dense_store.hpp"
#include "collapsing_lowest_dense_store.hpp"
#include "dense_store.hpp"

namespace datasketches {

TEST_CASE("ddsketch", "[ddsketch]") {
  std::cout << "ddsketch test" << std::endl;

  CollapsingHighestDenseStore<std::allocator<uint64_t>> s(1024);
  // const CollapsingHighestDenseStore<std::allocator<uint64_t>> other_store_hi(store_hi);
  // store_hi.merge(other_store_hi);
  //
  // CollapsingLowestDenseStore<std::allocator<uint64_t>> store_lo(1024);
  // const CollapsingLowestDenseStore<std::allocator<uint64_t>> other_store_lo(store_lo);
  // store_lo.merge(other_store_lo);

  for (int i = 0; i < 1024; ++i) {
    s.add(i, i * 10);
  }
  // CollapsingLowestDenseStore<std::allocator<uint64_t>> store_lo(1024);
  // const CollapsingLowestDenseStore<std::allocator<uint64_t>> other_store_lo(store_lo);
  // store_lo.merge(other_store_lo);
  // store_lo.add(1, 1);
  // store_lo.add(12, 2);
  // store_lo.add(23, 3);
  //
  // //store_hi.merge(store_lo);
  // for (const Bin& bin : store_hi) {
  //   std::cout << bin.toString() << std::endl;
  // }
}


} /* namespace datasketches */