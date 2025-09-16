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
#include "bin.hpp"
#include <iostream>
namespace datasketches {

void TestBinInitialization(const int index, const uint64_t count) {
  Bin bin(index, count);
  REQUIRE(bin.get_count() == count);
  REQUIRE(bin.get_index() == index);
}

TEST_CASE("bintest", "[bintest]") {
  TestBinInitialization(0, 1);
  TestBinInitialization(3, 1);
  TestBinInitialization(INT_MAX >> 1, 1);
  TestBinInitialization(INT_MAX, 1);
  TestBinInitialization(-3, 1);
  TestBinInitialization(INT_MIN >> 1, 1);
  TestBinInitialization(INT_MIN, 1);
}
} /* namespace datasketches */