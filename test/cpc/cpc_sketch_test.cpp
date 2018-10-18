/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include "cpc_sketch.hpp"

namespace datasketches {

static const double RELATIVE_ERROR_FOR_LG_K_11 = 0.02;

class cpc_sketch_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(cpc_sketch_test);
  CPPUNIT_TEST(lg_k_limits);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(one_value);
  CPPUNIT_TEST(many_values);
  CPPUNIT_TEST(serialize_deserialize_empty);
  CPPUNIT_TEST(serialize_deserialize_sparse);
  CPPUNIT_TEST(serialize_deserialize_hybrid);
  CPPUNIT_TEST(serialize_deserialize_pinned);
  CPPUNIT_TEST(serialize_deserialize_sliding);
  CPPUNIT_TEST(serialize_deserialize_empty_custom_seed);
  CPPUNIT_TEST(copy);
  CPPUNIT_TEST_SUITE_END();

  void lg_k_limits() {
    cpc_sketch s1(CPC_MIN_LG_K); // this should work
    cpc_sketch s2(CPC_MAX_LG_K); // this should work
    CPPUNIT_ASSERT_THROW(cpc_sketch s3(CPC_MIN_LG_K - 1), std::invalid_argument);
    CPPUNIT_ASSERT_THROW(cpc_sketch s4(CPC_MAX_LG_K + 1), std::invalid_argument);
  }

  void empty() {
    cpc_sketch sketch(11);
    CPPUNIT_ASSERT(sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(0.0, sketch.get_estimate());
  }

  void one_value() {
    cpc_sketch sketch(11);
    sketch.update(1);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, sketch.get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
  }

  void many_values() {
    cpc_sketch sketch(11);
    const int n(10000);
    for (int i = 0; i < n; i++) sketch.update(i);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, sketch.get_estimate(), n * RELATIVE_ERROR_FOR_LG_K_11);
    CPPUNIT_ASSERT(sketch.get_estimate() > sketch.get_lower_bound(1));
    CPPUNIT_ASSERT(sketch.get_estimate() < sketch.get_upper_bound(1));
  }

  void serialize_deserialize_empty() {
    cpc_sketch sketch(11);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto sketch_ptr(cpc_sketch::deserialize(s));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());

    std::ofstream os("cpc-empty.bin");
    sketch.serialize(os);
  }

  void serialize_deserialize_sparse() {
    cpc_sketch sketch(11);
    const int n(100);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto sketch_ptr(cpc_sketch::deserialize(s));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());

    std::ofstream os("cpc-sparse.bin");
    sketch.serialize(os);
  }

  void serialize_deserialize_hybrid() {
    cpc_sketch sketch(11);
    const int n(200);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto sketch_ptr(cpc_sketch::deserialize(s));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());

    std::ofstream os("cpc-hybrid.bin");
    sketch.serialize(os);
  }

  void serialize_deserialize_pinned() {
    cpc_sketch sketch(11);
    const int n(2000);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto sketch_ptr(cpc_sketch::deserialize(s));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());

    std::ofstream os("cpc-pinned.bin");
    sketch.serialize(os);
  }

  void serialize_deserialize_sliding() {
    cpc_sketch sketch(11);
    const int n(20000);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto sketch_ptr(cpc_sketch::deserialize(s));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());

    std::ofstream os("cpc-sliding.bin");
    sketch.serialize(os);
  }

  void copy() {
    cpc_sketch s1(11);
    s1.update(1);
    cpc_sketch s2 = s1; // copy constructor
    CPPUNIT_ASSERT(!s2.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, s2.get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
    s2.update(2);
    s1 = s2; // operator=
    CPPUNIT_ASSERT_DOUBLES_EQUAL(2, s1.get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
  }

  void serialize_deserialize_empty_custom_seed() {
    cpc_sketch sketch(11, 123);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto sketch_ptr(cpc_sketch::deserialize(s, 123));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());

    // incompatible seed
    CPPUNIT_ASSERT_THROW(cpc_sketch::deserialize(s), std::invalid_argument);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(cpc_sketch_test);

} /* namespace datasketches */
