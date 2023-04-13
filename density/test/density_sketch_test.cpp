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

#include <density_sketch.hpp>

namespace datasketches {

TEST_CASE("density sketch: empty", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);
  REQUIRE(sketch.is_empty());
  REQUIRE_THROWS_AS(sketch.get_estimate({0, 0, 0}), std::runtime_error);
}

TEST_CASE("density sketch: one item", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);

  // dimension mismatch
  REQUIRE_THROWS_AS(sketch.update(std::vector<float>({0, 0})), std::invalid_argument);

  sketch.update(std::vector<float>({0, 0, 0}));
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_estimate({0, 0, 0}) == 1);
  REQUIRE(sketch.get_estimate({0.01, 0.01, 0.01}) > 0.95);
  REQUIRE(sketch.get_estimate({1, 1, 1}) < 0.05);
}

TEST_CASE("density sketch: merge", "[density_sketch]") {
  density_sketch<float> sketch1(10, 4);
  sketch1.update(std::vector<float>({0, 0, 0, 0}));
  sketch1.update(std::vector<float>({1, 2, 3, 4}));

  density_sketch<float> sketch2(10, 4);
  sketch2.update(std::vector<float>({5, 6, 7, 8}));

  sketch1.merge(sketch2);

  REQUIRE(sketch1.get_n() == 3);
  REQUIRE(sketch1.get_num_retained() == 3);
}

TEST_CASE("density sketch: iterator", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);
  unsigned n = 1000;
  for (unsigned i = 1; i <= n; ++i) sketch.update(std::vector<float>(3, i));
  REQUIRE(sketch.get_n() == n);
  REQUIRE(sketch.is_estimation_mode());
  //std::cout << sketch.to_string(true, true);
  unsigned count = 0;
  for (auto pair: sketch) {
    ++count;
    // just to assert something about the output
    REQUIRE(pair.first.size() == sketch.get_dim());
  }
  REQUIRE(count == sketch.get_num_retained());
}

// spherical kernel for testing, returns 1 for vectors within radius and 0 otherwise
template<typename T>
struct spherical_kernel {
  spherical_kernel(T radius = 1.0) : _radius_squared(radius * radius) {}
  T operator()(const std::vector<T>& v1, const std::vector<T>& v2) const {
    return std::inner_product(v1.begin(), v1.end(), v2.begin(), 0.0, std::plus<T>(), [](T a, T b){return (a-b)*(a-b);}) <= _radius_squared ? 1.0 : 0.0;
  }
  private:
    T _radius_squared;
};

TEST_CASE("custom kernel", "[density_sketch]") {
  density_sketch<float, spherical_kernel<float>> sketch(10, 3, spherical_kernel<float>(0.5));

  // update with (1,1,1) and test points inside and outside the kernel
  sketch.update(std::vector<float>(3, 1.0));
  REQUIRE(sketch.get_estimate(std::vector<float>(3, 1.001)) == 1.0);
  REQUIRE(sketch.get_estimate(std::vector<float>(3, 2.0)) == 0.0);

  // rest of test follows iterator test above
  unsigned n = 1000;
  for (unsigned i = 2; i <= n; ++i) sketch.update(std::vector<float>(3, i));
  REQUIRE(sketch.get_n() == n);
  REQUIRE(sketch.is_estimation_mode());
  unsigned count = 0;
  for (auto pair: sketch) {
    ++count;
    // just to assert something about the output
    REQUIRE(pair.first.size() == sketch.get_dim());
  }
  REQUIRE(count == sketch.get_num_retained());
}

} /* namespace datasketches */
