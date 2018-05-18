/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <kll_sketch.hpp>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>
#include <cmath>
#include <cstring>

namespace sketches {

static const double RANK_EPS_FOR_K_200 = 0.0133;

class Test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(Test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(one_item);
  CPPUNIT_TEST(many_items_exact_mode);
  CPPUNIT_TEST(many_items_estimation_mode);
  CPPUNIT_TEST(deserialize_from_java);
  CPPUNIT_TEST(serialize_deserialize_empty);
  CPPUNIT_TEST(serialize_deserialize);
  CPPUNIT_TEST_SUITE_END();

  void empty() {
    kll_sketch sketch;
    CPPUNIT_ASSERT(sketch.is_empty());
    CPPUNIT_ASSERT(!sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0ull, sketch.get_n());
    CPPUNIT_ASSERT_EQUAL(0u, sketch.get_num_retained());
    CPPUNIT_ASSERT(std::isnan(sketch.get_rank(0)));
    CPPUNIT_ASSERT(std::isnan(sketch.get_min_value()));
    CPPUNIT_ASSERT(std::isnan(sketch.get_max_value()));
    CPPUNIT_ASSERT(std::isnan(sketch.get_quantile(0.5)));
  }

  void one_item() {
    kll_sketch sketch;
    sketch.update(1);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT(!sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1ull, sketch.get_n());
    CPPUNIT_ASSERT_EQUAL(1u, sketch.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(0.0, sketch.get_rank(1));
    CPPUNIT_ASSERT_EQUAL(1.0, sketch.get_rank(2));
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch.get_min_value());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch.get_max_value());
    CPPUNIT_ASSERT_EQUAL(1.0f, sketch.get_quantile(0.5));
  }

  void many_items_exact_mode() {
    kll_sketch sketch;
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

    for (uint32_t i = 0; i < n; i++) {
      const double trueRank = (double) i / n;
      CPPUNIT_ASSERT_EQUAL(trueRank, sketch.get_rank(i));
    }
  }

  void many_items_estimation_mode() {
    kll_sketch sketch;
    const int n(1000000);
    for (int i = 0; i < n; i++) {
      sketch.update(i);
      CPPUNIT_ASSERT_EQUAL((unsigned long long) i + 1, sketch.get_n());
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
      const float quantile = sketch.get_quantile(fractions[i]);
      CPPUNIT_ASSERT_EQUAL(quantile, quantiles[i]);
      CPPUNIT_ASSERT_EQUAL(quantile, reverse_quantiles[1000 - i]);
      CPPUNIT_ASSERT(previous_quantile <= quantile);
      previous_quantile = quantile;
    }

    std::cout << sketch << std::endl;
  }

  void deserialize_from_java() {
    std::ifstream is("src/kll_sketch_from_java.bin", std::ios::binary);
    auto sketch_ptr(kll_sketch::deserialize(is));
    CPPUNIT_ASSERT(!sketch_ptr->is_empty());
    CPPUNIT_ASSERT(sketch_ptr->is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1000000ull, sketch_ptr->get_n());
    CPPUNIT_ASSERT_EQUAL(614u, sketch_ptr->get_num_retained());
    CPPUNIT_ASSERT_EQUAL(0.0f, sketch_ptr->get_min_value());
    CPPUNIT_ASSERT_EQUAL(999999.0f, sketch_ptr->get_max_value());
  }

  void serialize_deserialize_empty() {
    kll_sketch sketch;
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch.get_serialized_size_bytes(), (uint32_t) s.tellp());
    auto sketch_ptr(kll_sketch::deserialize(s));
    CPPUNIT_ASSERT_EQUAL(sketch_ptr->get_serialized_size_bytes(), (uint32_t) s.tellg());
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.is_estimation_mode(), sketch_ptr->is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(sketch.get_n(), sketch_ptr->get_n());
    CPPUNIT_ASSERT_EQUAL(sketch.get_num_retained(), sketch_ptr->get_num_retained());
    CPPUNIT_ASSERT(std::isnan(sketch_ptr->get_min_value()));
    CPPUNIT_ASSERT(std::isnan(sketch_ptr->get_max_value()));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(false), sketch_ptr->get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(true), sketch_ptr->get_normalized_rank_error(true));
  }

  void serialize_deserialize() {
    kll_sketch sketch;
    const int n(1000);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch.get_serialized_size_bytes(), (uint32_t) s.tellp());
    auto sketch_ptr(kll_sketch::deserialize(s));
    CPPUNIT_ASSERT_EQUAL(sketch_ptr->get_serialized_size_bytes(), (uint32_t) s.tellg());
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.is_estimation_mode(), sketch_ptr->is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(sketch.get_n(), sketch_ptr->get_n());
    CPPUNIT_ASSERT_EQUAL(sketch.get_num_retained(), sketch_ptr->get_num_retained());
    CPPUNIT_ASSERT_EQUAL(sketch.get_min_value(), sketch_ptr->get_min_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_max_value(), sketch_ptr->get_max_value());
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(false), sketch_ptr->get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch.get_normalized_rank_error(true), sketch_ptr->get_normalized_rank_error(true));
    CPPUNIT_ASSERT_EQUAL(sketch.get_rank(0.5), sketch_ptr->get_rank(0.5));
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(Test);

}
