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

#include <theta_union_experimental.hpp>

namespace datasketches {

TEST_CASE("theta_union_exeperimental") {
  std::cout << "theta union test begin" << std::endl;
  auto update_sketch1 = theta_sketch_experimental<>::builder().build();
  update_sketch1.update(1);
  update_sketch1.update(2);

  auto update_sketch2 = theta_sketch_experimental<>::builder().build();
  update_sketch2.update(1);
  update_sketch2.update(3);

  auto u = theta_union_experimental<>::builder().build();
  u.update(update_sketch1);
  u.update(update_sketch2);
  auto r = u.get_result();
  std::cout << r.to_string(true);
  std::cout << "theta union test end" << std::endl;
}

} /* namespace datasketches */
