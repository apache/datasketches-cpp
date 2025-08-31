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

#include "index_mapping_factory.hpp"
#include "linearly_interpolated_mapping.hpp"

namespace datasketches {

constexpr double min_tested_relative_accuracy = 1e-8;
constexpr double max_tested_relative_accuracy = 1 - 1e-3;
constexpr double multiplier = 1 + std::numbers::sqrt2 * 1e2;

void assert_relative_accuracy(const double& expected, const double& actual, const double& relative_accuracy) {
  REQUIRE(expected >= 0);
  REQUIRE(actual >= 0);
  if (expected == 0) {
    REQUIRE(actual == Approx(0.).margin(1e-12));
  } else {
    REQUIRE(std::abs(expected - actual) / expected <= relative_accuracy + 1e-12);
  }
}

void test_accuracy(const IndexMapping& mapping, const double& relative_accuracy) {
  for (double value = mapping.min_indexable_value(); value < mapping.max_indexable_value(); value *= multiplier) {
    const double mapped_value = mapping.value(mapping.index(value));
    assert_relative_accuracy(value, mapped_value, relative_accuracy);
  }
  const double value = mapping.max_indexable_value();
  const double mapped_value = mapping.value(mapping.index(value));
  assert_relative_accuracy(value, mapped_value, relative_accuracy);
  REQUIRE(relative_accuracy <= mapping.get_relative_accuracy() + 1e-10);
}

TEMPLATE_TEST_CASE("test index mapping accuracy", "[indexmappingtest]",
  LinearlyInterpolatedMapping
  ) {
  for (double relative_accuracy = max_tested_relative_accuracy; relative_accuracy >= min_tested_relative_accuracy; relative_accuracy *= max_tested_relative_accuracy) {
    auto mapping = index_mapping_factory<TestType>::new_mapping(relative_accuracy);
    test_accuracy(*mapping, relative_accuracy);
  }
}
}
