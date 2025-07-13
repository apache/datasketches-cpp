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

#include <string>
#include <functional>
#include <iostream>
#include <catch2/catch.hpp>

#include "collapsing_lowest_dense_store.hpp"
#include "dense_store.hpp"
#include "sparse_store.hpp"

namespace datasketches {
using alloc = std::allocator<uint64_t>;


template<typename Allocator, class Store>
class StoreTest {
public:
  Store new_store();
};

TEST_CASE("DenseStore", "[DenseStore]") {

  StoreTest<std::allocator<uint64_t>> store_test;
  auto store = store_test.new_store();
  std::cout << "DenseStore test" << std::endl;
}

}
