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
#include <tuple_intersection.hpp>

namespace datasketches {

template<typename Summary>
struct subtracting_intersection_policy {
  void operator()(Summary& summary, const Summary& other) const {
    summary -= other;
  }
};

TEST_CASE("tuple_intersection float", "[tuple_intersection]") {
  auto update_sketch1 = update_tuple_sketch<float>::builder().build();
  update_sketch1.update(1, 1);
  update_sketch1.update(2, 1);

  auto update_sketch2 = update_tuple_sketch<float>::builder().build();
  update_sketch2.update(1, 1);
  update_sketch2.update(3, 1);

  tuple_intersection<float, subtracting_intersection_policy<float>> is;
  is.update(update_sketch1);
  is.update(update_sketch2);
  auto r = is.get_result();
  REQUIRE(r.get_num_retained() == 1);
}

} /* namespace datasketches */
