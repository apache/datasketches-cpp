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
#include <vector>
#include <random>
#include <algorithm>
#include <cmath>
#include <stdexcept>
#include <map>

#include "ddsketch.hpp"
#include "logarithmic_mapping.hpp"
#include "unbounded_size_dense_store.hpp"
#include "collapsing_highest_dense_store.hpp"
#include "collapsing_lowest_dense_store.hpp"

namespace datasketches {

using A = std::allocator<uint64_t>;
constexpr double EPSILON = 1e-10;

void assert_accurate(double min_expected, double max_expected, double actual, double relative_accuracy) {
  const double relaxed_min_expected = min_expected > 0 ? min_expected * (1 - relative_accuracy) : min_expected * (1 + relative_accuracy);
  const double relaxed_max_expected = max_expected > 0 ? max_expected * (1 + relative_accuracy) : max_expected * (1 - relative_accuracy);
  bool failed = (actual < relaxed_min_expected - EPSILON) || (actual > relaxed_max_expected + EPSILON);
  REQUIRE(!failed);
}

// Test helper functions
void assert_quantile_accurate(const std::vector<double>& sorted_values, double quantile, double actual_quantile_value, double relative_accuracy) {
  const double lower_quantile_value = sorted_values[static_cast<size_t>(std::floor(quantile * (sorted_values.size() - 1)))];
  const double upper_quantile_value = sorted_values[static_cast<size_t>(std::ceil(quantile * (sorted_values.size() - 1)))];

  assert_accurate(lower_quantile_value, upper_quantile_value, actual_quantile_value, relative_accuracy);
}


template<typename SketchType>
void assert_encodes(const SketchType& sketch, const std::vector<double>& values, double relative_accuracy) {
    REQUIRE(sketch.get_count() == Approx(values.size()).margin(EPSILON));

    if (values.empty()) {
        REQUIRE(sketch.is_empty());
        return;
    }

    REQUIRE_FALSE(sketch.is_empty());

    auto sorted_values = values;
    std::sort(sorted_values.begin(), sorted_values.end());

    const double min_value = sketch.get_min();
    const double max_value = sketch.get_max();

    assert_accurate(sorted_values[0], sorted_values[0], min_value, relative_accuracy);
    assert_accurate(sorted_values.back(), sorted_values.back(), max_value, relative_accuracy);

    // Test quantiles
    for (double quantile = 0.0; quantile <= 1.0; quantile += 0.01) {
        const double value_at_quantile = sketch.get_quantile(quantile);
        assert_quantile_accurate(sorted_values, quantile, value_at_quantile, relative_accuracy);

        REQUIRE(value_at_quantile >= min_value);
        REQUIRE(value_at_quantile <= max_value);
    }

    // Test sum accuracy (if values have same sign)
    if (sorted_values[0] >= 0 || sorted_values.back() <= 0) {
        const double expected_sum = std::accumulate(values.begin(), values.end(), 0.0);
        assert_accurate(expected_sum, expected_sum, sketch.get_sum(), relative_accuracy);
    }
}

template<typename SketchType>
void test_adding(SketchType& sketch, const std::vector<double>& values, double relative_accuracy) {
    // Test individual additions
    sketch.clear();
    for (const double& value : values) {
        sketch.update(value);
    }
    assert_encodes(sketch, values, relative_accuracy);

    // Test weighted additions
    sketch.clear();
    auto sketch_weighted(sketch);
    std::map<double, int> value_counts;
    for (const double& value : values) {
        value_counts[value]++;
    }

    for (const auto& [value, count] : value_counts) {
        sketch_weighted.update(value, count);
    }
    assert_encodes(sketch_weighted, values, relative_accuracy);
}

template<typename SketchType>
void test_merging(SketchType& sketch, const std::vector<std::vector<double>>& value_arrays, double relative_accuracy) {
  sketch.clear();
  for (const auto& values : value_arrays) {
    SketchType intermediate_sketch(sketch);
    intermediate_sketch.clear();
    for (const double& value : values) {
      intermediate_sketch.update(value);
    }
    sketch.merge(intermediate_sketch);
  }

  // Flatten all values
  std::vector<double> all_values;
  for (const auto& values : value_arrays) {
      all_values.insert(all_values.end(), values.begin(), values.end());
  }

  assert_encodes(sketch, all_values, relative_accuracy);
}

using DDSketchUnboundedStoreTestCase = std::pair<store_factory<UnboundedSizeDenseStore<A>>, LogarithmicMapping>;

template<int N>
using DDSketchCollapsingHighestStoreTestCase = std::pair<store_factory<CollapsingHighestDenseStore<N, A>>, LogarithmicMapping>;

template<int N>
using DDSketchCollapsingLowestStoreTestCase = std::pair<store_factory<CollapsingLowestDenseStore<N, A>>, LogarithmicMapping>;

TEMPLATE_TEST_CASE("DDSketch empty test", "[ddsketch]",
  DDSketchUnboundedStoreTestCase,
  DDSketchCollapsingHighestStoreTestCase<128>,
  DDSketchCollapsingLowestStoreTestCase<128>
) {
  auto positive_store = *TestType::first_type::new_store();
  auto negative_store = *TestType::first_type::new_store();
  using StoreType = decltype(positive_store);
  using MappingType = typename TestType::second_type;

  constexpr double relative_accuracy = 0.01;
  DDSketch<StoreType, MappingType> sketch(relative_accuracy);;

  REQUIRE(sketch.is_empty());
  REQUIRE(sketch.get_count() == Approx(0.0).margin(EPSILON));
  REQUIRE_THROWS_AS(sketch.get_min(), std::runtime_error);
  REQUIRE_THROWS_AS(sketch.get_max(), std::runtime_error);
  REQUIRE_THROWS_AS(sketch.get_quantile(0.5), std::runtime_error);
}
//
// TEMPLATE_TEST_CASE("DDSketch exception test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<128>,
//   DDSketchCollapsingLowestStoreTestCase<128>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//   constexpr double relative_accuracy = 0.01;
//   DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//
//   // Test invalid quantile values
//   sketch.update(1.0);
//   REQUIRE_THROWS_AS(sketch.get_quantile(-0.1), std::invalid_argument);
//   REQUIRE_THROWS_AS(sketch.get_quantile(1.1), std::invalid_argument);
//
//   // Test invalid count values
//   REQUIRE_THROWS_AS(sketch.update(1.0, -1.0), std::invalid_argument);
// }
//
// TEMPLATE_TEST_CASE("DDSketch clear test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<128>,
//   DDSketchCollapsingLowestStoreTestCase<128>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//   constexpr double relative_accuracy = 0.01;
//   DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//
//   sketch.update(1.0);
//   sketch.update(2.0);
//   sketch.clear();
//
//   REQUIRE(sketch.is_empty());
//   REQUIRE(sketch.get_count() == Approx(0.0).margin(EPSILON));
// }
//
// TEMPLATE_TEST_CASE("DDSketch constant test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<128>,
//   DDSketchCollapsingLowestStoreTestCase<128>
// ){
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//
//   for (double relative_accuracy = 1e-1; relative_accuracy >= 1e-3; relative_accuracy *= 1e-1) {
//     DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {0.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {1.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {1.0, 1.0, 1.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {10.0, 10.0, 10.0}, relative_accuracy);
//
//     std::vector<double> large_constant(10000, 2.0);
//     test_adding<decltype(sketch)>(sketch, large_constant, relative_accuracy);
//   }
// }
//
// TEMPLATE_TEST_CASE("DDSketch negative constants test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<128>,
//   DDSketchCollapsingLowestStoreTestCase<128>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//
//   for (double relative_accuracy = 1e-1; relative_accuracy >= 1e-3; relative_accuracy *= 1e-1) {
//     DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {0.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {-1.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {-1.0, -1.0, -1.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {-10.0, -10.0, -10.0}, relative_accuracy);
//
//     // Large negative constant array
//     std::vector<double> large_negative(10000, -2.0);
//     test_adding<decltype(sketch)>(sketch, large_negative, relative_accuracy);
//   }
// }
// TEMPLATE_TEST_CASE("DDSketch mixed positive negative test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<128>,
//   DDSketchCollapsingLowestStoreTestCase<128>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//
//   for (double relative_accuracy = 1e-1; relative_accuracy >= 1e-3; relative_accuracy *= 1e-1) {
//     DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {0.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {-1.0, 1.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {-1.0, -1.0, -1.0, 1.0, 1.0, 1.0}, relative_accuracy);
//     test_adding<decltype(sketch)>(sketch, {-10.0, -10.0, -10.0, 10.0, 10.0, 10.0}, relative_accuracy);
//
//     // Large negative constant array
//     std::vector<double> mixed_large;
//     mixed_large.reserve(20000);
//     for (int i = 0; i < 20000; ++i) {
//       mixed_large.push_back(i % 2 == 0 ? 2.0 : -2.0);
//     }
//     std::vector<double> large_negative(10000, -2.0);
//     test_adding<decltype(sketch)>(sketch, large_negative, relative_accuracy);
//   }
// }
// TEMPLATE_TEST_CASE("DDSketch with zeros test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<4096>,
//   DDSketchCollapsingLowestStoreTestCase<4096>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//
//   for (double relative_accuracy = 1e-1; relative_accuracy >= 1e-3 ; relative_accuracy *= 1e-1) {
//     DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//
//     // All zeros
//     std::vector<double> all_zeros(100, 0.0);
//     test_adding<decltype(sketch)>(sketch, all_zeros, relative_accuracy);
//
//     // Zeros at beginning
//     std::vector<double> zeros_beginning(110, 0.0);
//     for (int i = 0; i < 100; ++i) {
//       zeros_beginning[10+i] = i;
//     }
//     test_adding<decltype(sketch)>(sketch, zeros_beginning, relative_accuracy);
//     // Zeros at end
//     std::vector<double> zeros_end(110, 0.0);
//     zeros_end.reserve(110);
//     for (int i = 0; i < 10; ++i) {
//       zeros_end[zeros_end.size() - 1] = i;
//     }
//     test_adding<decltype(sketch)>(sketch, zeros_end, relative_accuracy);
//   }
// }
//
// TEMPLATE_TEST_CASE("DDSketch linear sequences test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//
//   for (double relative_accuracy = 1e-1; relative_accuracy >= 1e-3 ; relative_accuracy *= 1e-1) {
//     DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//     // Increasing linearly
//     std::vector<double> increasing;
//     increasing.reserve(10000);
//     for (int i = 0; i < 10000; ++i) {
//       increasing.push_back(i);
//     }
//     test_adding<decltype(sketch)>(sketch, increasing, relative_accuracy);
//
//     // Decreasing linearly
//     std::vector<double> decreasing;
//     decreasing.reserve(10000);
//     for (int i = 0; i < 10000; ++i) {
//       decreasing.push_back(10000 - i);
//     }
//     test_adding<decltype(sketch)>(sketch, decreasing, relative_accuracy);
//
//     // Negative increasing
//     std::vector<double> negative_increasing;
//     negative_increasing.reserve(10000);
//     for (int i = -10000; i < 0; ++i) {
//       negative_increasing.push_back(i);
//     }
//     test_adding<decltype(sketch)>(sketch, negative_increasing, relative_accuracy);
//
//     // Mixed positive/negative increasing
//     std::vector<double> mixed_increasing;
//     for (int i = -10000; i < 10000; ++i) {
//       mixed_increasing.push_back(i);
//     }
//     test_adding<decltype(sketch)>(sketch, mixed_increasing, relative_accuracy);
//   }
// }
//
// TEMPLATE_TEST_CASE("DDSketch exponential sequence test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//
//   for (double relative_accuracy = 1e-1; relative_accuracy >= 1e-3 ; relative_accuracy *= 1e-1) {
//     DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//     // Increasing exponentially
//     std::vector<double> increasing_exp;
//     increasing_exp.reserve(100);
//     for (int i = 0; i < 100; ++i) {
//       increasing_exp.push_back(std::exp(i));
//     }
//     test_adding<decltype(sketch)>(sketch, increasing_exp, relative_accuracy);
//
//     // Decreasing exponentially
//     std::vector<double> decreasing;
//     decreasing.reserve(100);
//     for (int i = 0; i < 100; ++i) {
//       decreasing.push_back(std::exp(- i));
//     }
//     test_adding<decltype(sketch)>(sketch, decreasing, relative_accuracy);
//
//     // Negative increasing
//     std::vector<double> negative_increasing;
//     negative_increasing.reserve(100);
//     for (int i = -100; i < 0; ++i) {
//       negative_increasing.push_back(-std::exp(i));
//     }
//     test_adding<decltype(sketch)>(sketch, negative_increasing, relative_accuracy);
//   }
// }
//
// TEMPLATE_TEST_CASE("DDSketch merging test", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<4096>,
//   DDSketchCollapsingLowestStoreTestCase<4096>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//
//   for (double relative_accuracy = 1e-1; relative_accuracy >= 1e-1 ; relative_accuracy *= 1e-1) {
//     DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//     // Test merging empty sketches
//     test_merging<decltype(sketch)>(sketch, {{}, {}}, relative_accuracy);
//     test_merging<decltype(sketch)>(sketch, {{}, {0.0}}, relative_accuracy);
//     test_merging<decltype(sketch)>(sketch, {{0.0}, {}}, relative_accuracy);
//
//     // Test merging constants
//     test_merging<decltype(sketch)>(sketch, {{1.0, 1.0}, {1.0, 1.0, 1.0}}, relative_accuracy);
//
//     // Test merging far apart values
//     test_merging<decltype(sketch)>(sketch, {{0.0}, {10000.0}}, relative_accuracy);
//     test_merging<decltype(sketch)>(sketch, {{10000.0}, {20000.0}}, relative_accuracy);
//     test_merging<decltype(sketch)>(sketch, {{20000.0}, {10000.0}}, relative_accuracy);
//   }
// }
//
// TEMPLATE_TEST_CASE("DDSketch different mappings", "[ddsketch]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<4096>,
//   DDSketchCollapsingLowestStoreTestCase<4096>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//   constexpr double relative_accuracy = 0.01;
//   std::vector<double> test_values = {0.0, 1.0, -1.0, 10.0, -10.0, 100.0, -100.0};
//   DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//   test_adding<decltype(sketch)>(sketch, test_values, relative_accuracy);
// }
//
// TEMPLATE_TEST_CASE("DDSketch add random test", "[ddsketch][random]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<4096>,
//   DDSketchCollapsingLowestStoreTestCase<4096>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//   constexpr double relative_accuracy = 0.01;
//   constexpr int num_tests = 100;
//   constexpr int max_num_values = 1000;
//
//   DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//   std::random_device rd;
//   std::mt19937_64 rng(rd());
//   std::uniform_int_distribution<int> size_dist(0, max_num_values - 1);
//   std::uniform_real_distribution<double> value_dist(-1000.0, 1000.0);
//
//   for (int i = 0; i < num_tests; ++i) {
//     std::vector<double> values;
//     int num_values = size_dist(rng);
//     values.reserve(num_values);
//
//     for (int j = 0; j < num_values; ++j) {
//         values.push_back(value_dist(rng));
//     }
//
//     test_adding<decltype(sketch)>(sketch, values, relative_accuracy);
//   }
// }
//
// TEMPLATE_TEST_CASE("DDSketch merging random test", "[ddsketch][random]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<4096>,
//   DDSketchCollapsingLowestStoreTestCase<4096>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//   constexpr double relative_accuracy = 0.01;
//   constexpr int num_tests = 100;
//   constexpr int max_num_sketches = 100;
//   constexpr int max_num_values_per_sketch = 1000;
//
//   DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//   std::random_device rd;
//   std::mt19937_64 rng(rd());
//   std::uniform_int_distribution<int> sketch_size_dist(0, max_num_sketches - 1);
//   std::uniform_int_distribution<int> values_size_dist(0, max_num_values_per_sketch - 1);
//   std::uniform_real_distribution<double> value_dist(-1000.0, 1000.0);
//
//   for (int i = 0; i < num_tests; ++i) {
//     std::vector<std::vector<double>> value_arrays;
//     int num_sketches = sketch_size_dist(rng);
//     value_arrays.reserve(num_sketches);
//
//     for (int j = 0; j < num_sketches; ++j) {
//       std::vector<double> values;
//       int num_values = values_size_dist(rng);
//       values.reserve(num_values);
//
//       for (int k = 0; k < num_values; ++k) {
//         values.push_back(value_dist(rng));
//       }
//       value_arrays.push_back(std::move(values));
//     }
//
//     test_merging<decltype(sketch)>(sketch, value_arrays, relative_accuracy);
//   }
// }
//
// TEMPLATE_TEST_CASE("DDSketch serialize - deserialize test", "[ddsketch][random]",
//   DDSketchUnboundedStoreTestCase,
//   DDSketchCollapsingHighestStoreTestCase<4096>,
//   DDSketchCollapsingLowestStoreTestCase<4096>
// ) {
//   auto positive_store = *TestType::first_type::new_store();
//   auto negative_store = *TestType::first_type::new_store();
//   using StoreType = decltype(positive_store);
//   using MappingType = typename TestType::second_type;
//   constexpr double relative_accuracy = 0.01;
//   constexpr int num_tests = 100;
//   constexpr int max_num_values = 1000;
//
//   DDSketch<StoreType, MappingType> sketch(relative_accuracy);
//   std::random_device rd;
//   std::mt19937_64 rng(rd());
//   std::uniform_int_distribution<int> size_dist(0, max_num_values - 1);
//   std::uniform_real_distribution<double> value_dist(-1000.0, 1000.0);
//
//   std::stringstream ss;
//   sketch.serialize(ss);
//   DDSketch<StoreType, MappingType> deserialized_empty_sketch = DDSketch<StoreType, MappingType>::deserialize(ss);
//   REQUIRE(sketch.is_empty());
//   REQUIRE(deserialized_empty_sketch.is_empty());
//   REQUIRE(ss.peek() == std::istream::traits_type::eof());
//   REQUIRE(sketch == deserialized_empty_sketch);
//   ss.clear();
//
//   for (int i = 0; i < num_tests; ++i) {
//     std::vector<double> values;
//     int num_values = size_dist(rng);
//
//     for (int j = 0; j < num_values; ++j) {
//       sketch.update(value_dist(rng));
//     }
//
//     sketch.serialize(ss);
//     auto deserialized_sketch = DDSketch<StoreType, MappingType>::deserialize(ss);
//     REQUIRE_FALSE(sketch.is_empty());
//     REQUIRE_FALSE(deserialized_sketch.is_empty());
//     REQUIRE(ss.peek() == std::istream::traits_type::eof());
//     REQUIRE(sketch == deserialized_sketch);
//     ss.clear();
//
//   }
//
//
// }
//
// TEST_CASE("quantile", "[ddsketch]") {
//   std::random_device rd{};
//   std::mt19937_64 gen{rd()};
//   std::normal_distribution<double> d(0.0, 1.0);
//
//   DDSketch<CollapsingHighestDenseStore<1024, A>, LogarithmicMapping> ddsketch(0.01);
//   for (size_t i = 0; i < 1000000; ++i) {
//     double val = d(gen);
//     ddsketch.update(val);
//   }
//
//   DDSketch<CollapsingLowestDenseStore<8, A>, LinearlyInterpolatedMapping> sk(0.01);
//
//   // std::cout << ddsketch.get_quantile(0.5) << std::endl;
//   // std::cout << ddsketch.get_rank(4) << std::endl;
//
//   std::cout << ddsketch.to_string();
// }
} /* namespace datasketches */