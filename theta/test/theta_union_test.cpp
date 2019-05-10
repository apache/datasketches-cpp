/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <theta_union.hpp>

namespace datasketches {

class theta_union_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(theta_union_test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(exact_mode_half_overlap);
  CPPUNIT_TEST(estimation_mode_half_overlap);
  CPPUNIT_TEST_SUITE_END();

  void empty() {
    update_theta_sketch sketch1 = update_theta_sketch::builder().build();
    theta_union u = theta_union::builder().build();
    u.update(sketch1);
    compact_theta_sketch sketch2 = u.get_result();
    CPPUNIT_ASSERT(sketch2.is_empty());
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

};

CPPUNIT_TEST_SUITE_REGISTRATION(theta_union_test);

} /* namespace datasketches */
