/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <theta_sketch.hpp>

namespace datasketches {

class theta_sketch_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(theta_sketch_test);
  CPPUNIT_TEST(test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(single_item);
  CPPUNIT_TEST(resize_exact);
  CPPUNIT_TEST(estimation);
  CPPUNIT_TEST(deserialize_compact_empty_from_java);
  CPPUNIT_TEST(deserialize_single_item_from_java);
  CPPUNIT_TEST(deserialize_compact_estimation_from_java);
  CPPUNIT_TEST_SUITE_END();

  void test() {
    update_theta_sketch update_sketch = update_theta_sketch::builder().build();
    std::cerr << "after build" << std::endl;
    std::ofstream ofs("theta_update_empty.bin");
    update_sketch.serialize(ofs);

    CPPUNIT_ASSERT(update_sketch.is_empty());

    int value = 1;
    update_sketch.update(value++);
    CPPUNIT_ASSERT(!update_sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(1.0, update_sketch.get_estimate());

    theta_sketch& sketch1 = update_sketch;
    CPPUNIT_ASSERT_EQUAL(1.0, sketch1.get_estimate());

    compact_theta_sketch compact_sketch = update_sketch.compact();
    CPPUNIT_ASSERT_EQUAL(1.0, compact_sketch.get_estimate());
    compact_sketch.to_stream(std::cerr, true);

    theta_sketch& sketch2 = compact_sketch;
    CPPUNIT_ASSERT_EQUAL(1.0, sketch2.get_estimate());

    std::cerr << "copy construct" << std::endl;

    update_theta_sketch sketch3 = update_sketch;

    std::cerr << "move construct" << std::endl;

    update_theta_sketch sketch4 = std::move(update_sketch);

    std::cerr << "copy assign" << std::endl;

    sketch3 = sketch4;

    std::cerr << "move" << std::endl;

    sketch3 = std::move(sketch4);

    std::cerr << "end" << std::endl;

    sketch3.update(value++);
    sketch3.to_stream(std::cerr, true);

    update_theta_sketch usk = update_theta_sketch::builder().build();
    for (int i = 0; i < 10000; i++) usk.update(i);
    std::ofstream ofs2("theta_update_estimation.bin");
    usk.serialize(ofs2);
  }

  void empty() {
    update_theta_sketch update_sketch = update_theta_sketch::builder().build();
    CPPUNIT_ASSERT(update_sketch.is_empty());
    CPPUNIT_ASSERT(!update_sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1.0, update_sketch.get_theta());
    CPPUNIT_ASSERT_EQUAL(0.0, update_sketch.get_estimate());
  }

  void single_item() {
    update_theta_sketch update_sketch = update_theta_sketch::builder().build();
    update_sketch.update(1);
    CPPUNIT_ASSERT(!update_sketch.is_empty());
    CPPUNIT_ASSERT(!update_sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1.0, update_sketch.get_theta());
    CPPUNIT_ASSERT_EQUAL(1.0, update_sketch.get_estimate());
  }

  void resize_exact() {
    update_theta_sketch update_sketch = update_theta_sketch::builder().build();
    for (int i = 0; i < 2000; i++) update_sketch.update(i);
    CPPUNIT_ASSERT(!update_sketch.is_empty());
    CPPUNIT_ASSERT(!update_sketch.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1.0, update_sketch.get_theta());
    CPPUNIT_ASSERT_EQUAL(2000.0, update_sketch.get_estimate());
  }

  void estimation() {
    update_theta_sketch update_sketch = update_theta_sketch::builder().set_resize_factor(update_theta_sketch::resize_factor::X1).build();
    const int n = 8000;
    for (int i = 0; i < n; i++) update_sketch.update(i);
    CPPUNIT_ASSERT(!update_sketch.is_empty());
    CPPUNIT_ASSERT(update_sketch.is_estimation_mode());
    CPPUNIT_ASSERT(update_sketch.get_theta() < 1.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL((double) n, update_sketch.get_estimate(), n * 0.01);
  }

  void deserialize_compact_empty_from_java() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open("test/theta_compact_empty_from_java.bin", std::ios::binary);
    auto sketchptr = theta_sketch::deserialize(is);
    CPPUNIT_ASSERT(sketchptr->is_empty());
    CPPUNIT_ASSERT(!sketchptr->is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(0U, sketchptr->get_num_retained());
    CPPUNIT_ASSERT_EQUAL(1.0, sketchptr->get_theta());
    CPPUNIT_ASSERT_EQUAL(0.0, sketchptr->get_estimate());
  }

  void deserialize_single_item_from_java() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open("test/theta_compact_single_item_from_java.bin", std::ios::binary);
    auto sketchptr = theta_sketch::deserialize(is);
    CPPUNIT_ASSERT(!sketchptr->is_empty());
    CPPUNIT_ASSERT(!sketchptr->is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(1U, sketchptr->get_num_retained());
    CPPUNIT_ASSERT_EQUAL(1.0, sketchptr->get_theta());
    CPPUNIT_ASSERT_EQUAL(1.0, sketchptr->get_estimate());
  }

  void deserialize_compact_estimation_from_java() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open("test/theta_compact_estimation_from_java.bin", std::ios::binary);
    auto sketchptr = theta_sketch::deserialize(is);
    CPPUNIT_ASSERT(!sketchptr->is_empty());
    CPPUNIT_ASSERT(sketchptr->is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(4342U, sketchptr->get_num_retained());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.531700444213199, sketchptr->get_theta(), 1e-10);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(8166.25234614053, sketchptr->get_estimate(), 1e-10);

    update_theta_sketch update_sketch = update_theta_sketch::builder().build();
    const int n = 8192;
    for (int i = 0; i < n; i++) update_sketch.update(i);
    CPPUNIT_ASSERT_EQUAL(update_sketch.get_num_retained(), sketchptr->get_num_retained());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(update_sketch.get_theta(), sketchptr->get_theta(), 1e-10);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(update_sketch.get_estimate(), sketchptr->get_estimate(), 1e-10);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(theta_sketch_test);

} /* namespace datasketches */
