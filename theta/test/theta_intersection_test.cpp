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

#include <theta_intersection.hpp>

namespace datasketches {

class theta_intersection_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(theta_intersection_test);
  CPPUNIT_TEST(invalid);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(non_empty_no_retained_keys);
  CPPUNIT_TEST(exact_mode_half_overlap_unordered);
  CPPUNIT_TEST(exact_mode_half_overlap_ordered);
  CPPUNIT_TEST(exact_mode_disjoint_unordered);
  CPPUNIT_TEST(exact_mode_disjoint_ordered);
  CPPUNIT_TEST(estimation_mode_half_overlap_unordered);
  CPPUNIT_TEST(estimation_mode_half_overlap_ordered);
  CPPUNIT_TEST(estimation_mode_disjoint_unordered);
  CPPUNIT_TEST(estimation_mode_disjoint_ordered);
  CPPUNIT_TEST(seed_mismatch);
  CPPUNIT_TEST_SUITE_END();

  void invalid() {
    theta_intersection intersection;
    CPPUNIT_ASSERT(!intersection.has_result());
    CPPUNIT_ASSERT_THROW(intersection.get_result(), std::invalid_argument);
  }

  void empty() {
    theta_intersection intersection;
    update_theta_sketch sketch = update_theta_sketch::builder().build();
    intersection.update(sketch);
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT_EQUAL(0U, result.get_num_retained());
    CPPUNIT_ASSERT(result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());

    intersection.update(sketch);
    result = intersection.get_result();
    CPPUNIT_ASSERT_EQUAL(0U, result.get_num_retained());
    CPPUNIT_ASSERT(result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void non_empty_no_retained_keys() {
    update_theta_sketch sketch = update_theta_sketch::builder().set_p(0.001).build();
    sketch.update(1);
    theta_intersection intersection;
    intersection.update(sketch);
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT_EQUAL(0U, result.get_num_retained());
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.001, result.get_theta(), 1e-10);
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());

    intersection.update(sketch);
    result = intersection.get_result();
    CPPUNIT_ASSERT_EQUAL(0U, result.get_num_retained());
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.001, result.get_theta(), 1e-10);
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void exact_mode_half_overlap_unordered() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 1000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    value = 500;
    for (int i = 0; i < 1000; i++) sketch2.update(value++);

    theta_intersection intersection;
    intersection.update(sketch1);
    intersection.update(sketch2);
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(500.0, result.get_estimate());
  }

  void exact_mode_half_overlap_ordered() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 1000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    value = 500;
    for (int i = 0; i < 1000; i++) sketch2.update(value++);

    theta_intersection intersection;
    intersection.update(sketch1.compact());
    intersection.update(sketch2.compact());
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(500.0, result.get_estimate());
  }

  void exact_mode_disjoint_unordered() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 1000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    for (int i = 0; i < 1000; i++) sketch2.update(value++);

    theta_intersection intersection;
    intersection.update(sketch1);
    intersection.update(sketch2);
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT(result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void exact_mode_disjoint_ordered() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 1000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    for (int i = 0; i < 1000; i++) sketch2.update(value++);

    theta_intersection intersection;
    intersection.update(sketch1.compact());
    intersection.update(sketch2.compact());
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT(result.is_empty());
    CPPUNIT_ASSERT(!result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void estimation_mode_half_overlap_unordered() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 10000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    value = 5000;
    for (int i = 0; i < 10000; i++) sketch2.update(value++);

    theta_intersection intersection;
    intersection.update(sketch1);
    intersection.update(sketch2);
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(5000, result.get_estimate(), 5000 * 0.02);
  }

  void estimation_mode_half_overlap_ordered() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 10000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    value = 5000;
    for (int i = 0; i < 10000; i++) sketch2.update(value++);

    theta_intersection intersection;
    intersection.update(sketch1.compact());
    intersection.update(sketch2.compact());
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(5000, result.get_estimate(), 5000 * 0.02);
  }

  void estimation_mode_disjoint_unordered() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 10000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    for (int i = 0; i < 10000; i++) sketch2.update(value++);

    theta_intersection intersection;
    intersection.update(sketch1);
    intersection.update(sketch2);
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void estimation_mode_disjoint_ordered() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 10000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    for (int i = 0; i < 10000; i++) sketch2.update(value++);

    theta_intersection intersection;
    intersection.update(sketch1.compact());
    intersection.update(sketch2.compact());
    compact_theta_sketch result = intersection.get_result();
    CPPUNIT_ASSERT(!result.is_empty());
    CPPUNIT_ASSERT(result.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0.0, result.get_estimate());
  }

  void seed_mismatch() {
    update_theta_sketch sketch = update_theta_sketch::builder().build();
    sketch.update(1); // non-empty should not be ignored
    theta_intersection intersection(123);
    CPPUNIT_ASSERT_THROW(intersection.update(sketch), std::invalid_argument);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(theta_intersection_test);

} /* namespace datasketches */
