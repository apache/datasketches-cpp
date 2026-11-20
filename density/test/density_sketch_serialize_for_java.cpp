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
#include <cmath>
#include <fstream>
#include <string>
#include <vector>

#include "density_sketch.hpp"

namespace datasketches {

// The same inputs as the datasketches-rust generator (datagen/src/density.rs),
// so that each C++ snapshot has a Rust counterpart with the same name

TEST_CASE("density sketch float generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000};
  for (const unsigned n: n_arr) {
    density_sketch<float> sketch(10, 3);
    for (unsigned i = 0; i < n; ++i) {
      const float x = static_cast<float>(i);
      sketch.update(std::vector<float>({x, std::sqrt(x), -x}));
    }
    std::ofstream os("density_float_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.serialize(os);
  }
}

TEST_CASE("density sketch double generate", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000};
  for (const unsigned n: n_arr) {
    density_sketch<double> sketch(10, 3);
    for (unsigned i = 0; i < n; ++i) {
      const double x = static_cast<double>(i);
      sketch.update(std::vector<double>({x, std::sqrt(x), -x}));
    }
    std::ofstream os("density_double_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    sketch.serialize(os);
  }
}

} /* namespace datasketches */
