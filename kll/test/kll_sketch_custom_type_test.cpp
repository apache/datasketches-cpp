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

#include <kll_sketch.hpp>
#include <test_allocator.hpp>
#include <test_type.hpp>


namespace datasketches {

typedef kll_sketch<test_type, test_type_less, test_type_serde, test_allocator<test_type>> kll_test_type_sketch;

class kll_sketch_custom_type_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(kll_sketch_custom_type_test);
  CPPUNIT_TEST(compact_level_zero);
  CPPUNIT_TEST(merge_small);
  CPPUNIT_TEST(merge_higher_levels);
  CPPUNIT_TEST(serialize_deserialize);
  CPPUNIT_TEST_SUITE_END();

public:
  void setUp() {
    test_allocator_total_bytes = 0;
  }

  void tearDown() {
    if (test_allocator_total_bytes != 0) {
      CPPUNIT_ASSERT_EQUAL((long long) 0, test_allocator_total_bytes);
    }
  }

  void compact_level_zero() {
    kll_test_type_sketch sketch(8);
    CPPUNIT_ASSERT_THROW(sketch.get_quantile(0), std::runtime_error);
    CPPUNIT_ASSERT_THROW(sketch.get_min_value(), std::runtime_error);
    CPPUNIT_ASSERT_THROW(sketch.get_max_value(), std::runtime_error);
    CPPUNIT_ASSERT_EQUAL(8u, sketch.get_serialized_size_bytes());

    sketch.update(1);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7);
    sketch.update(8);
    sketch.update(9);

    //sketch.to_stream(std::cout);

    CPPUNIT_ASSERT(sketch.is_estimation_mode());
    CPPUNIT_ASSERT(sketch.get_n() > sketch.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(1, sketch.get_min_value().get_value());
    CPPUNIT_ASSERT_EQUAL(9, sketch.get_max_value().get_value());
  }

  void merge_small() {
    kll_test_type_sketch sketch1(8);
    sketch1.update(1);

    kll_test_type_sketch sketch2(8);
    sketch2.update(2);

    sketch2.merge(sketch1);

    //sketch2.to_stream(std::cout);

    CPPUNIT_ASSERT(!sketch2.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL((int) sketch2.get_n(), (int) sketch2.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(1, sketch2.get_min_value().get_value());
    CPPUNIT_ASSERT_EQUAL(2, sketch2.get_max_value().get_value());
  }

  void merge_higher_levels() {
    kll_test_type_sketch sketch1(8);
    sketch1.update(1);
    sketch1.update(2);
    sketch1.update(3);
    sketch1.update(4);
    sketch1.update(5);
    sketch1.update(6);
    sketch1.update(7);
    sketch1.update(8);
    sketch1.update(9);

    kll_test_type_sketch sketch2(8);
    sketch2.update(10);
    sketch2.update(11);
    sketch2.update(12);
    sketch2.update(13);
    sketch2.update(14);
    sketch2.update(15);
    sketch2.update(16);
    sketch2.update(17);
    sketch2.update(18);

    sketch2.merge(sketch1);

    //sketch2.to_stream(std::cout);

    CPPUNIT_ASSERT(sketch2.is_estimation_mode());
    CPPUNIT_ASSERT(sketch2.get_n() > sketch2.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(1, sketch2.get_min_value().get_value());
    CPPUNIT_ASSERT_EQUAL(18, sketch2.get_max_value().get_value());
  }

  void serialize_deserialize() {
    kll_test_type_sketch sketch1;

    const int n = 1000;
    for (int i = 0; i < n; i++) sketch1.update(i);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch1.serialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch1.get_serialized_size_bytes(), (uint32_t) s.tellp());
    auto sketch2 = kll_test_type_sketch::deserialize(s);
    CPPUNIT_ASSERT_EQUAL(sketch2.get_serialized_size_bytes(), (uint32_t) s.tellg());
    CPPUNIT_ASSERT_EQUAL(s.tellp(), s.tellg());
    CPPUNIT_ASSERT_EQUAL(sketch1.is_empty(), sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch1.is_estimation_mode(), sketch2.is_estimation_mode());
    CPPUNIT_ASSERT_EQUAL(sketch1.get_n(), sketch2.get_n());
    CPPUNIT_ASSERT_EQUAL(sketch1.get_num_retained(), sketch2.get_num_retained());
    CPPUNIT_ASSERT_EQUAL(sketch1.get_min_value().get_value(), sketch2.get_min_value().get_value());
    CPPUNIT_ASSERT_EQUAL(sketch1.get_max_value().get_value(), sketch2.get_max_value().get_value());
    CPPUNIT_ASSERT_EQUAL(sketch1.get_normalized_rank_error(false), sketch2.get_normalized_rank_error(false));
    CPPUNIT_ASSERT_EQUAL(sketch1.get_normalized_rank_error(true), sketch2.get_normalized_rank_error(true));
    CPPUNIT_ASSERT_EQUAL(sketch1.get_quantile(0.5).get_value(), sketch2.get_quantile(0.5).get_value());
    CPPUNIT_ASSERT_EQUAL(sketch1.get_rank(0), sketch2.get_rank(0));
    CPPUNIT_ASSERT_EQUAL(sketch1.get_rank(n), sketch2.get_rank(n));
    CPPUNIT_ASSERT_EQUAL(sketch1.get_rank(n / 2), sketch2.get_rank(n / 2));
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(kll_sketch_custom_type_test);

} /* namespace datasketches */
