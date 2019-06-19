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

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <theta_a_not_b.hpp>

namespace datasketches {

class theta_a_not_b_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(theta_a_not_b_test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(non_empty_no_retained_keys);
  CPPUNIT_TEST(exact_mode_half_overlap);
  CPPUNIT_TEST(exact_mode_disjoint);
  CPPUNIT_TEST(exact_mode_full_overlap);
  CPPUNIT_TEST(estimation_mode_half_overlap);
  CPPUNIT_TEST(estimation_mode_disjoint);
  CPPUNIT_TEST(estimation_mode_full_overlap);
  CPPUNIT_TEST(seed_mismatch);
  CPPUNIT_TEST_SUITE_END();

  void empty() {
    theta_a_not_b a_not_b;
    update_theta_sketch a = update_theta_sketch::builder().build();
    update_theta_sketch b = update_theta_sketch::builder().build();
    compact_theta_sketch result = a_not_b.compute(a, b);
    CPPUNIT_ASSERT_EQUAL(0U, result.get_num_retained());
    CPPUNIT_ASSERT(result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void non_empty_no_retained_keys() {
    update_theta_sketch a = update_theta_sketch::builder().build();
    a.update(1);
    update_theta_sketch b = update_theta_sketch::builder().set_p(0.001).build();
    theta_a_not_b a_not_b;

    // B is still empty
    compact_theta_sketch result = a_not_b.compute(a, b);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1U, result.get_num_retained());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, result.get_theta(), 1e-10);
    CPPUNIT_ASSERT_EQUAL(1.0, result.get_estimate());

    // B is not empty in estimation mode and no entries
    b.update(1);
    CPPUNIT_ASSERT_EQUAL(0U, b.get_num_retained());

    result = a_not_b.compute(a, b);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0U, result.get_num_retained());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.001, result.get_theta(), 1e-10);
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void exact_mode_half_overlap() {
    update_theta_sketch a = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 1000; i++) a.update(value++);

    update_theta_sketch b = update_theta_sketch::builder().build();
    value = 500;
    for (int i = 0; i < 1000; i++) b.update(value++);

    theta_a_not_b a_not_b;

    // unordered inputs, ordered result
    compact_theta_sketch result = a_not_b.compute(a, b);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT(result.is_ordered());
    CPPUNIT_ASSERT_EQUAL(500.0, result.get_estimate());

    // unordered inputs, unordered result
    result = a_not_b.compute(a, b, false);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT(!result.is_ordered());
    CPPUNIT_ASSERT_EQUAL(500.0, result.get_estimate());

    // ordered inputs
    result = a_not_b.compute(a.compact(), b.compact());
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT(result.is_ordered());
    CPPUNIT_ASSERT_EQUAL(500.0, result.get_estimate());

    // A is ordered, so the result is ordered regardless
    result = a_not_b.compute(a.compact(), b, false);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT(result.is_ordered());
    CPPUNIT_ASSERT_EQUAL(500.0, result.get_estimate());
}

  void exact_mode_disjoint() {
    update_theta_sketch a = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 1000; i++) a.update(value++);

    update_theta_sketch b = update_theta_sketch::builder().build();
    for (int i = 0; i < 1000; i++) b.update(value++);

    theta_a_not_b a_not_b;

    // unordered inputs
    compact_theta_sketch result = a_not_b.compute(a, b);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1000.0, result.get_estimate());

    // ordered inputs
    result = a_not_b.compute(a.compact(), b.compact());
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1000.0, result.get_estimate());
  }

  void exact_mode_full_overlap() {
    update_theta_sketch sketch = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 1000; i++) sketch.update(value++);

    theta_a_not_b a_not_b;

    // unordered inputs
    compact_theta_sketch result = a_not_b.compute(sketch, sketch);
    CPPUNIT_ASSERT(result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());

    // ordered inputs
    result = a_not_b.compute(sketch.compact(), sketch.compact());
    CPPUNIT_ASSERT(result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void estimation_mode_half_overlap() {
    update_theta_sketch a = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 10000; i++) a.update(value++);

    update_theta_sketch b = update_theta_sketch::builder().build();
    value = 5000;
    for (int i = 0; i < 10000; i++) b.update(value++);

    theta_a_not_b a_not_b;

    // unordered inputs
    compact_theta_sketch result = a_not_b.compute(a, b);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(5000, result.get_estimate(), 5000 * 0.02);

    // ordered inputs
    result = a_not_b.compute(a.compact(), b.compact());
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(5000, result.get_estimate(), 5000 * 0.02);
  }

  void estimation_mode_disjoint() {
    update_theta_sketch a = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 10000; i++) a.update(value++);

    update_theta_sketch b = update_theta_sketch::builder().build();
    for (int i = 0; i < 10000; i++) b.update(value++);

    theta_a_not_b a_not_b;

    // unordered inputs
    compact_theta_sketch result = a_not_b.compute(a, b);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(10000, result.get_estimate(), 10000 * 0.02);

    // ordered inputs
    result = a_not_b.compute(a.compact(), b.compact());
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(10000, result.get_estimate(), 10000 * 0.02);
  }

  void estimation_mode_full_overlap() {
    update_theta_sketch sketch = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 10000; i++) sketch.update(value++);

    theta_a_not_b a_not_b;

    // unordered inputs
    compact_theta_sketch result = a_not_b.compute(sketch, sketch);
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());

    // ordered inputs
    result = a_not_b.compute(sketch.compact(), sketch.compact());
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void seed_mismatch() {
    update_theta_sketch sketch = update_theta_sketch::builder().build();
    sketch.update(1); // non-empty should not be ignored
    theta_a_not_b a_not_b(123);
    CPPUNIT_ASSERT_THROW(a_not_b.compute(sketch, sketch), std::invalid_argument);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(theta_a_not_b_test);

} /* namespace datasketches */
