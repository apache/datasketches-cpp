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

#include <theta_union.hpp>

namespace datasketches {

class theta_union_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(theta_union_test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(non_empty_no_retained_keys);
  CPPUNIT_TEST(exact_mode_half_overlap);
  CPPUNIT_TEST(estimation_mode_half_overlap);
  CPPUNIT_TEST(seed_mismatch);
  CPPUNIT_TEST_SUITE_END();

  void empty() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    theta_union u = theta_union::builder().build();
    compact_theta_sketch sketch2 = u.get_result();
    CPPUNIT_ASSERT_EQUAL(0U, sketch2.get_num_retained());
    CPPUNIT_ASSERT(sketch2.is_empty());
    CPPUNIT_ASSERT(!sketch2.is_estimation_mode());

    u.update(sketch1);
    sketch2 = u.get_result();
    CPPUNIT_ASSERT_EQUAL(0U, sketch2.get_num_retained());
    CPPUNIT_ASSERT(sketch2.is_empty());
    CPPUNIT_ASSERT(!sketch2.is_estimation_mode());
  }

  void non_empty_no_retained_keys() {
    update_theta_sketch update_sketch = update_theta_sketch::builder().set_p(0.001).build();
    update_sketch.update(1);
    theta_union u = theta_union::builder().build();
    u.update(update_sketch);
    compact_theta_sketch sketch = u.get_result();
    CPPUNIT_ASSERT_EQUAL(0U, sketch.get_num_retained());
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT(sketch.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.001, sketch.get_theta(), 1e-10);
  }

  void exact_mode_half_overlap() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 1000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    value = 500;
    for (int i = 0; i < 1000; i++) sketch2.update(value++);

    theta_union u = theta_union::builder().build();
    u.update(sketch1);
    u.update(sketch2);
    compact_theta_sketch sketch3 = u.get_result();
    CPPUNIT_ASSERT(!sketch3.is_empty());
    CPPUNIT_ASSERT(!sketch3.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1500, sketch3.get_estimate(), 1500 * 0.01);
  }

  void estimation_mode_half_overlap() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    int value = 0;
    for (int i = 0; i < 10000; i++) sketch1.update(value++);

    update_theta_sketch sketch2 = update_theta_sketch::builder().build();
    value = 5000;
    for (int i = 0; i < 10000; i++) sketch2.update(value++);

    theta_union u = theta_union::builder().build();
    u.update(sketch1);
    u.update(sketch2);
    compact_theta_sketch sketch3 = u.get_result();
    CPPUNIT_ASSERT(!sketch3.is_empty());
    CPPUNIT_ASSERT(sketch3.is_estimation_mode());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(15000, sketch3.get_estimate(), 15000 * 0.01);
    //sketch3.to_stream(std::cerr, true);
  }

  void seed_mismatch() {
    update_theta_sketch sketch = update_theta_sketch::builder().build();
    sketch.update(1); // non-empty should not be ignored
    theta_union u = theta_union::builder().set_seed(123).build();
    CPPUNIT_ASSERT_THROW(u.update(sketch), std::invalid_argument);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(theta_union_test);

} /* namespace datasketches */
