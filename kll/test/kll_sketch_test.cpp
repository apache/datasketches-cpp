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

#include <kll_sketch.hpp>
#include <kll_helper.hpp>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cmath>
#include <cstring>

namespace datasketches {

static const double RANK_EPS_FOR_K_200 = 0.0133;
static const double NUMERIC_NOISE_TOLERANCE = 1E-6;

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

class kll_sketch_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(kll_sketch_test);
  CPPUNIT_TEST(k_limits);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(bad_get_quantile);
  CPPUNIT_TEST(one_item);
  CPPUNIT_TEST(many_items_exact_mode);
  CPPUNIT_TEST(many_items_estimation_mode);
  CPPUNIT_TEST(consistency_between_get_rank_and_get_PMF_CDF);
  CPPUNIT_TEST(deserialize_from_java);
  CPPUNIT_TEST(serialize_deserialize_empty);
  CPPUNIT_TEST(serialize_deserialize_one_item);
  CPPUNIT_TEST(deserialize_one_item_v1);
  CPPUNIT_TEST(serialize_deserialize_stream);
  CPPUNIT_TEST(serialize_deserialize_bytes);
  CPPUNIT_TEST(floor_of_log2_of_fraction);
  CPPUNIT_TEST(out_of_order_split_points_float);
  CPPUNIT_TEST(out_of_order_split_points_int);
  CPPUNIT_TEST(nan_split_point);
  CPPUNIT_TEST(merge);
  CPPUNIT_TEST(merge_lower_k);
  CPPUNIT_TEST(merge_exact_mode_lower_k);
  CPPUNIT_TEST(merge_min_value_from_other);
  CPPUNIT_TEST(merge_min_and_max_from_other);
  CPPUNIT_TEST(sketch_of_ints);
  CPPUNIT_TEST(sketch_of_strings);
  CPPUNIT_TEST(copy);
  CPPUNIT_TEST_SUITE_END();

  void k_limits() {
    kll_sketch<float> sketch1(kll_sketch<float>::MIN_K); // this should work
    kll_sketch<float> sketch2(kll_sketch<float>::MAX_K); // this should work
    CPPUNIT_ASSERT_THROW(new kll_sketch<float>(kll_sketch<float>::MIN_K - 1), std::invalid_argument);
    // MAX_K + 1 makes no sense because k is uint16_t
  }

  void empty() {
    kll_sketch<float> sketch;
    CPPUNIT_ASSERT(sketch.is_empty());
    CPPUNIT_ASSERT(!sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 0, sketch.get_n());
    CPPUNIT_ASSERT_EQUAL(0u, sketch.get_num_retained());
    CPPUNIT_ASSERT(std::isnan(sketch.get_rank(0)));
    CPPUNIT_ASSERT(std::isnan(sketch.get_min_value()));
    CPPUNIT_ASSERT(std::isnan(sketch.get_max_value()));
    CPPUNIT_ASSERT(std::isnan(sketch.get_quantile(0.5)));
    const double fractions[3] {0, 0.5, 1};
    CPPUNIT_ASSERT(!sketch.get_quantiles(fractions, 3));
    const float split_points[1] {0};
    CPPUNIT_ASSERT(!sketch.get_PMF(split_points, 1));
    CPPUNIT_ASSERT(!sketch.get_CDF(split_points, 1));

    int count = 0;
    for (auto& it: sketch) {
      (void) it; // to suppress "unused" warning
      ++count;
    }
    CPPUNIT_ASSERT_EQUAL(0, count);
}

  void bad_get_quantile() {
    kll_sketch<float> sketch;
    sketch.update(0); // has to be non-empty to reach the check
    CPPUNIT_ASSERT_THROW(sketch.get_quantile(-1), std::invalid_argument);
  }

  void one_item() {
    kll_sketch<float> sketch;
    sketch.update(1);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT(!sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 1, sketch.get_n());
    CPPUNIT_ASSERT_EQUAL(1u, sketch.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(0.0, sketch.get_rank(1));
    CPPUNIT_ASSERT_EQUAL(1.0, sketch.get_rank(2));
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch.get_min_value());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch.get_max_value());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch.get_quantile(0.5));
    const double fractions[3] {0, 0.5, 1};
    auto quantiles(sketch.get_quantiles(fractions, 3));
    CPPUNIT_ASSERT(quantiles);
    CPPUNIT_ASSERT_EQUAL(1.0f, quantiles[0]);
    CPPUNIT_ASSERT_EQUAL(1.0f, quantiles[1]);
    CPPUNIT_ASSERT_EQUAL(1.0f, quantiles[2]);

    int count = 0;
    for (auto& it: sketch) {
      CPPUNIT_ASSERT_EQUAL(1, (int) it.second);
      ++count;
    }
    CPPUNIT_ASSERT_EQUAL(1, count);
  }

  void many_items_exact_mode() {
    kll_sketch<float> sketch;
    const uint32_t n(200);
    for (uint32_t i = 0; i < n; i++) {
      sketch.update(i);
      CPPUNIT_ASSERT_EQUAL((uint64_t) i + 1, sketch.get_n());
    }
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT(!sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(n, sketch.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch.get_min_value());
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch.get_quantile(0));
    CPPUNIT_ASSERT_EQUAL((float) n - 1, sketch.get_max_value());
    CPPUNIT_ASSERT_EQUAL((float) n - 1, sketch.get_quantile(1));

    const double fractions[3] {0, 0.5, 1};
    auto quantiles(sketch.get_quantiles(fractions, 3));
    CPPUNIT_ASSERT(quantiles);
    CPPUNIT_ASSERT_EQUAL(0.0f, quantiles[0]);
    CPPUNIT_ASSERT_EQUAL((float) n / 2, quantiles[1]);
    CPPUNIT_ASSERT_EQUAL((float) n - 1 , quantiles[2]);

    for (uint32_t i = 0; i < n; i++) {
      const double trueRank = (double) i / n;
      CPPUNIT_ASSERT_EQUAL(trueRank, sketch.get_rank(i));
    }
  }

  void many_items_estimation_mode() {
    kll_sketch<float> sketch;
    const int n(1000000);
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      CPPUNIT_ASSERT_EQUAL((uint64_t) i + 1, sketch.get_n());
    }
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT(sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch.get_min_value()); // min value is exact
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch.get_quantile(0)); // min value is exact
    CPPUNIT_ASSERT_EQUAL((float) n - 1, sketch.get_max_value()); // max value is exact
    CPPUNIT_ASSERT_EQUAL((float) n - 1, sketch.get_quantile(1)); // max value is exact

    // test rank
    for (int i = 0; i < n; i++) {
      const double trueRank = (double) i / n;
      CPPUNIT_ASSERT_DOUBLES_EQUAL(trueRank, sketch.get_rank(i), RANK_EPS_FOR_K_200);
    }

    // test quantiles at every 0.1 percentage point
    double fractions[1001];
    double reverse_fractions[1001]; // check that ordering does not matter
    for (int i = 0; i < 1001; i++) {
      fractions[i] = (double) i / 1000;
      reverse_fractions[1000 - i] = fractions[i];
    }
    auto quantiles = sketch.get_quantiles(fractions, 1001);
    auto reverse_quantiles = sketch.get_quantiles(reverse_fractions, 1001);
    float previous_quantile(0);
    for (int i = 0; i < 1001; i++) {
      // expensive in a loop, just to check the equivalence here, not advised for real code
      const float quantile = sketch.get_quantile(fractions[i]);
      CPPUNIT_ASSERT_EQUAL(quantile, quantiles[i]);
      CPPUNIT_ASSERT_EQUAL(quantile, reverse_quantiles[1000 - i]);
      CPPUNIT_ASSERT(previous_quantile <= quantile);
      previous_quantile = quantile;
    }

    //sketch.to_stream(std::cout);
  }

  void consistency_between_get_rank_and_get_PMF_CDF() {
    kll_sketch<float> sketch;
    const int n = 1000;
    float values[n];
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      values[i] = i;
    }

    const auto ranks(sketch.get_CDF(values, n));
    const auto pmf(sketch.get_PMF(values, n));

    double subtotal_pmf(0);
    for (int i = 0; i < n; i++) {
      CPPUNIT_ASSERT_EQUAL_MESSAGE("rank vs CDF for value " + std::to_string(i), ranks[i], sketch.get_rank(values[i]));
      subtotal_pmf += pmf[i];
      CPPUNIT_ASSERT_DOUBLES_EQUAL_MESSAGE("CDF vs PMF for value " + std::to_string(i), ranks[i], subtotal_pmf, NUMERIC_NOISE_TOLERANCE);
    }
  }

  void deserialize_from_java() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "kll_sketch_from_java.bin", std::ios::binary);
    auto sketch = kll_sketch<float>::deserialize(is);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT(sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 1000000, sketch.get_n());
    CPPUNIT_ASSERT_EQUAL(614u, sketch.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch.get_min_value());
    CPPUNIT_ASSERT_EQUAL(999999.0f, sketch.get_max_value());
  }

  void serialize_deserialize_empty() {
    kll_sketch<float> sketch;
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch.get_serialized_size_bytes(), (uint32_t) s.tellp());
    auto sketch2 = kll_sketch<float>::deserialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch2.get_serialized_size_bytes(), (uint32_t) s.tellg());
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.is_estimation_mode(), sketch2.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(sketch.get_n(), sketch2.get_n());
    CPPUNIT_ASSERT_EQUAL(sketch.get_num_retained(), sketch2.get_num_retained());
    CPPUNIT_ASSERT(std::isnan(sketch2.get_min_value()));
    CPPUNIT_ASSERT(std::isnan(sketch2.get_max_value()));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(false), sketch2.get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(true), sketch2.get_normalized_rank_error(true));
  }

  void serialize_deserialize_one_item() {
    kll_sketch<float> sketch;
    sketch.update(1);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch.get_serialized_size_bytes(), (uint32_t) s.tellp());
    auto sketch2 = kll_sketch<float>::deserialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch2.get_serialized_size_bytes(), (uint32_t) s.tellg());
    CPPUNIT_ASSERT_EQUAL(s.tellp(), s.tellg());
    CPPUNIT_ASSERT(!sketch2.is_empty());
    CPPUNIT_ASSERT(!sketch2.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 1, sketch2.get_n());
    CPPUNIT_ASSERT_EQUAL(1u, sketch2.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch2.get_max_value());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch2.get_quantile(0.5));
    CPPUNIT_ASSERT_EQUAL(0.0, sketch2.get_rank(1));
    CPPUNIT_ASSERT_EQUAL(1.0, sketch2.get_rank(2));
  }

  void deserialize_one_item_v1() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "kll_sketch_float_one_item_v1.bin", std::ios::binary);
    auto sketch = kll_sketch<float>::deserialize(is);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT(!sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 1, sketch.get_n());
    CPPUNIT_ASSERT_EQUAL(1u, sketch.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch.get_min_value());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch.get_max_value());
  }

  void serialize_deserialize_stream() {
    kll_sketch<float> sketch;
    const int n(1000);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch.get_serialized_size_bytes(), (uint32_t) s.tellp());
    auto sketch2 = kll_sketch<float>::deserialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch2.get_serialized_size_bytes(), (uint32_t) s.tellg());
    CPPUNIT_ASSERT_EQUAL(s.tellp(), s.tellg());
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.is_estimation_mode(), sketch2.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(sketch.get_n(), sketch2.get_n());
    CPPUNIT_ASSERT_EQUAL(sketch.get_num_retained(), sketch2.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(sketch.get_min_value(), sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_max_value(), sketch2.get_max_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(false), sketch2.get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(true), sketch2.get_normalized_rank_error(true));
    CPPUNIT_ASSERT_EQUAL(sketch.get_quantile(0.5), sketch2.get_quantile(0.5));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(0), sketch2.get_rank(0));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(n), sketch2.get_rank(n));
  }

  void serialize_deserialize_bytes() {
    kll_sketch<float> sketch;
    const int n(1000);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto data = sketch.serialize();
    CPPUNIT_ASSERT_EQUAL(sketch.get_serialized_size_bytes(), (uint32_t) data.second);
    auto sketch2 = kll_sketch<float>::deserialize(data.first.get(), data.second);
    CPPUNIT_ASSERT_EQUAL(sketch2.get_serialized_size_bytes(), (uint32_t) data.second);
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.is_estimation_mode(), sketch2.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(sketch.get_n(), sketch2.get_n());
    CPPUNIT_ASSERT_EQUAL(sketch.get_num_retained(), sketch2.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(sketch.get_min_value(), sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_max_value(), sketch2.get_max_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(false), sketch2.get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(true), sketch2.get_normalized_rank_error(true));
    CPPUNIT_ASSERT_EQUAL(sketch.get_quantile(0.5), sketch2.get_quantile(0.5));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(0), sketch2.get_rank(0));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(n), sketch2.get_rank(n));
  }

  void floor_of_log2_of_fraction() {
    CPPUNIT_ASSERT_EQUAL((uint8_t) 0, kll_helper::floor_of_log2_of_fraction(0, 1));
    CPPUNIT_ASSERT_EQUAL((uint8_t) 0, kll_helper::floor_of_log2_of_fraction(1, 2));
    CPPUNIT_ASSERT_EQUAL((uint8_t) 0, kll_helper::floor_of_log2_of_fraction(2, 2));
    CPPUNIT_ASSERT_EQUAL((uint8_t) 0, kll_helper::floor_of_log2_of_fraction(3, 2));
    CPPUNIT_ASSERT_EQUAL((uint8_t) 1, kll_helper::floor_of_log2_of_fraction(4, 2));
    CPPUNIT_ASSERT_EQUAL((uint8_t) 1, kll_helper::floor_of_log2_of_fraction(5, 2));
    CPPUNIT_ASSERT_EQUAL((uint8_t) 1, kll_helper::floor_of_log2_of_fraction(6, 2));
    CPPUNIT_ASSERT_EQUAL((uint8_t) 1, kll_helper::floor_of_log2_of_fraction(7, 2));
    CPPUNIT_ASSERT_EQUAL((uint8_t) 2, kll_helper::floor_of_log2_of_fraction(8, 2));
  }

  void out_of_order_split_points_float() {
    kll_sketch<float> sketch;
    sketch.update(0); // has too be non-empty to reach the check
    float split_points[2] = {1, 0};
    CPPUNIT_ASSERT_THROW(sketch.get_CDF(split_points, 2), std::invalid_argument);
  }

  void out_of_order_split_points_int() {
    kll_sketch<int> sketch;
    sketch.update(0); // has too be non-empty to reach the check
    int split_points[2] = {1, 0};
    CPPUNIT_ASSERT_THROW(sketch.get_CDF(split_points, 2), std::invalid_argument);
  }

  void nan_split_point() {
    kll_sketch<float> sketch;
    sketch.update(0); // has too be non-empty to reach the check
    float split_points[1] = {std::numeric_limits<float>::quiet_NaN()};
    CPPUNIT_ASSERT_THROW(sketch.get_CDF(split_points, 1), std::invalid_argument);
  }

  void merge() {
    kll_sketch<float> sketch1;
    kll_sketch<float> sketch2;
    const int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update((2 * n) - i - 1);
    }

    CPPUNIT_ASSERT_EQUAL(0.0f, sketch1.get_min_value());
    CPPUNIT_ASSERT_EQUAL((float) n - 1, sketch1.get_max_value());
    CPPUNIT_ASSERT_EQUAL((float) n, sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(2.0f * n - 1, sketch2.get_max_value());

    sketch1.merge(sketch2);

    CPPUNIT_ASSERT(!sketch1.is_empty());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 2 * n, sketch1.get_n());
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch1.get_min_value());
    CPPUNIT_ASSERT_EQUAL(2.0f * n - 1, sketch1.get_max_value());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, sketch1.get_quantile(0.5), n * RANK_EPS_FOR_K_200);
  }

  void merge_lower_k() {
    kll_sketch<float> sketch1(256);
    kll_sketch<float> sketch2(128);
    const int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
      sketch2.update((2 * n) - i - 1);
    }

    CPPUNIT_ASSERT_EQUAL(0.0f, sketch1.get_min_value());
    CPPUNIT_ASSERT_EQUAL((float) n - 1, sketch1.get_max_value());
    CPPUNIT_ASSERT_EQUAL((float) n, sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(2.0f * n - 1, sketch2.get_max_value());

    CPPUNIT_ASSERT(sketch1.get_normalized_rank_error(false) < sketch2.get_normalized_rank_error(false));
    CPPUNIT_ASSERT(sketch1.get_normalized_rank_error(true) < sketch2.get_normalized_rank_error(true));

    sketch1.merge(sketch2);

    // sketch1 must get "contaminated" by the lower K in sketch2
    CPPUNIT_ASSERT_EQUAL(sketch1.get_normalized_rank_error(false), sketch2.get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch1.get_normalized_rank_error(true), sketch2.get_normalized_rank_error(true));

    CPPUNIT_ASSERT(!sketch1.is_empty());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 2 * n, sketch1.get_n());
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch1.get_min_value());
    CPPUNIT_ASSERT_EQUAL(2.0f * n - 1, sketch1.get_max_value());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, sketch1.get_quantile(0.5), n * RANK_EPS_FOR_K_200);
  }

  void merge_exact_mode_lower_k() {
    kll_sketch<float> sketch1(256);
    kll_sketch<float> sketch2(128);
    const int n = 10000;
    for (int i = 0; i < n; i++) {
      sketch1.update(i);
    }

    // rank error should not be affected by a merge with an empty sketch with lower k
    const double rank_error_before_merge = sketch1.get_normalized_rank_error(true);
    sketch1.merge(sketch2);
    CPPUNIT_ASSERT_EQUAL(rank_error_before_merge, sketch1.get_normalized_rank_error(true));

    CPPUNIT_ASSERT(!sketch1.is_empty());
    CPPUNIT_ASSERT_EQUAL((uint64_t) n, sketch1.get_n());
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch1.get_min_value());
    CPPUNIT_ASSERT_EQUAL((float) n - 1, sketch1.get_max_value());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n / 2, sketch1.get_quantile(0.5), n / 2 * RANK_EPS_FOR_K_200);

    sketch2.update(0);
    sketch1.merge(sketch2);
    // rank error should not be affected by a merge with a sketch in exact mode with lower k
    CPPUNIT_ASSERT_EQUAL(rank_error_before_merge, sketch1.get_normalized_rank_error(true));
  }

  void merge_min_value_from_other() {
    kll_sketch<float> sketch1;
    kll_sketch<float> sketch2;
    sketch1.update(1);
    sketch2.update(2);
    sketch2.merge(sketch1);
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(2.0f, sketch2.get_max_value());
  }

  void merge_min_and_max_from_other() {
    kll_sketch<float> sketch1;
    for (int i = 0; i < 1000000; i++) sketch1.update(i);
    kll_sketch<float> sketch2;
    sketch2.merge(sketch1);
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(999999.0f, sketch2.get_max_value());
  }

  void sketch_of_ints() {
    kll_sketch<int> sketch;
    CPPUNIT_ASSERT_THROW(sketch.get_quantile(0), std::runtime_error);
    CPPUNIT_ASSERT_THROW(sketch.get_min_value(), std::runtime_error);
    CPPUNIT_ASSERT_THROW(sketch.get_max_value(), std::runtime_error);

    const int n(1000);
    for (int i = 0; i < n; i++) sketch.update(i);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch.get_serialized_size_bytes(), (uint32_t) s.tellp());
    auto sketch2 = kll_sketch<int>::deserialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch2.get_serialized_size_bytes(), (uint32_t) s.tellg());
    CPPUNIT_ASSERT_EQUAL(s.tellp(), s.tellg());
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.is_estimation_mode(), sketch2.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(sketch.get_n(), sketch2.get_n());
    CPPUNIT_ASSERT_EQUAL(sketch.get_num_retained(), sketch2.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(sketch.get_min_value(), sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_max_value(), sketch2.get_max_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(false), sketch2.get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(true), sketch2.get_normalized_rank_error(true));
    CPPUNIT_ASSERT_EQUAL(sketch.get_quantile(0.5), sketch2.get_quantile(0.5));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(0), sketch2.get_rank(0));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(n), sketch2.get_rank(n));
  }

  void sketch_of_strings() {
    kll_sketch<std::string> sketch;
    CPPUNIT_ASSERT_THROW(sketch.get_quantile(0), std::runtime_error);
    CPPUNIT_ASSERT_THROW(sketch.get_min_value(), std::runtime_error);
    CPPUNIT_ASSERT_THROW(sketch.get_max_value(), std::runtime_error);
    CPPUNIT_ASSERT_EQUAL(8u, sketch.get_serialized_size_bytes());

    const int n(1000);
    for (int i = 0; i < n; i++) sketch.update(std::to_string(i));

    CPPUNIT_ASSERT_EQUAL(std::string("0"), sketch.get_min_value());
    CPPUNIT_ASSERT_EQUAL(std::string("999"), sketch.get_max_value());

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch.get_serialized_size_bytes(), (uint32_t) s.tellp());
    auto sketch2 = kll_sketch<std::string>::deserialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch2.get_serialized_size_bytes(), (uint32_t) s.tellg());
    CPPUNIT_ASSERT_EQUAL(s.tellp(), s.tellg());
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.is_estimation_mode(), sketch2.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(sketch.get_n(), sketch2.get_n());
    CPPUNIT_ASSERT_EQUAL(sketch.get_num_retained(), sketch2.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(sketch.get_min_value(), sketch2.get_min_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_max_value(), sketch2.get_max_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(false), sketch2.get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(true), sketch2.get_normalized_rank_error(true));
    CPPUNIT_ASSERT_EQUAL(sketch.get_quantile(0.5), sketch2.get_quantile(0.5));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(std::to_string(0)), sketch2.get_rank(std::to_string(0)));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(std::to_string(n)), sketch2.get_rank(std::to_string(n)));

    // to take a look using hexdump
    std::ofstream os("kll-string.bin");
    sketch.serialize(os);

    // debug print
    //sketch.to_stream(std::cout);
  }

  void copy() {
    kll_sketch<int> sketch1;
    const int n(1000);
    for (int i = 0; i < n; i++) sketch1.update(i);

    // copy constructor
    kll_sketch<int> sketch2(sketch1);
    for (int i = 0; i < n; i++) {
      CPPUNIT_ASSERT_EQUAL(sketch1.get_rank(i), sketch2.get_rank(i));
    }

    // copy assignment
    kll_sketch<int> sketch3;
    sketch3 = sketch1;
    for (int i = 0; i < n; i++) {
      CPPUNIT_ASSERT_EQUAL(sketch1.get_rank(i), sketch3.get_rank(i));
    }
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(kll_sketch_test);

} /* namespace datasketches */
