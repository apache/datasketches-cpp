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

#include <cstring>

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
  CPPUNIT_TEST(serialize_deserialize_empty_bytes);
  CPPUNIT_TEST(serialize_deserialize_sparse_bytes);
  CPPUNIT_TEST(serialize_deserialize_hybrid_bytes);
  CPPUNIT_TEST(serialize_deserialize_pinned_bytes);
  CPPUNIT_TEST(serialize_deserialize_sliding_bytes);
  CPPUNIT_TEST(serialize_deserialize_empty_custom_seed);
  CPPUNIT_TEST(copy);
  CPPUNIT_TEST(kappa_range);
  CPPUNIT_TEST(validate_fail);
  CPPUNIT_TEST(serialize_both_ways);
  CPPUNIT_TEST(update_int_equivalence);
  CPPUNIT_TEST(update_float_equivalience);
  CPPUNIT_TEST(update_string_equivalence);
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
    CPPUNIT_ASSERT_EQUAL(0.0, sketch.get_lower_bound(1));
    CPPUNIT_ASSERT_EQUAL(0.0, sketch.get_upper_bound(1));
    CPPUNIT_ASSERT(sketch.validate());
  }

  void one_value() {
    cpc_sketch sketch(11);
    sketch.update(1);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, sketch.get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
    CPPUNIT_ASSERT(sketch.get_estimate() >= sketch.get_lower_bound(1));
    CPPUNIT_ASSERT(sketch.get_estimate() <= sketch.get_upper_bound(1));
    CPPUNIT_ASSERT(sketch.validate());
  }

  void many_values() {
    cpc_sketch sketch(11);
    const int n(10000);
    for (int i = 0; i < n; i++) sketch.update(i);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(n, sketch.get_estimate(), n * RELATIVE_ERROR_FOR_LG_K_11);
    CPPUNIT_ASSERT(sketch.get_estimate() >= sketch.get_lower_bound(1));
    CPPUNIT_ASSERT(sketch.get_estimate() <= sketch.get_upper_bound(1));
    CPPUNIT_ASSERT(sketch.validate());
  }

  void serialize_deserialize_empty() {
    cpc_sketch sketch(11);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    auto sketch_ptr(cpc_sketch::deserialize(s));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

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
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // updating again with the same values should not change the sketch
    for (int i = 0; i < n; i++) sketch_ptr->update(i);
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

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
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // updating again with the same values should not change the sketch
    for (int i = 0; i < n; i++) sketch_ptr->update(i);
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

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
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // updating again with the same values should not change the sketch
    for (int i = 0; i < n; i++) sketch_ptr->update(i);
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

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
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // updating again with the same values should not change the sketch
    for (int i = 0; i < n; i++) sketch_ptr->update(i);
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

    std::ofstream os("cpc-sliding.bin");
    sketch.serialize(os);
  }

  void serialize_deserialize_empty_bytes() {
    cpc_sketch sketch(11);
    auto data = sketch.serialize();
    auto sketch_ptr(cpc_sketch::deserialize(data.first.get(), data.second));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

    std::ofstream os("cpc-empty.bin");
    sketch.serialize(os);
  }

  void serialize_deserialize_hybrid_bytes() {
    cpc_sketch sketch(11);
    const int n(200);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto data = sketch.serialize();
    auto sketch_ptr(cpc_sketch::deserialize(data.first.get(), data.second));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // updating again with the same values should not change the sketch
    for (int i = 0; i < n; i++) sketch_ptr->update(i);
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());
  }

  void serialize_deserialize_sparse_bytes() {
    cpc_sketch sketch(11);
    const int n(100);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto data = sketch.serialize();
    auto sketch_ptr(cpc_sketch::deserialize(data.first.get(), data.second));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // updating again with the same values should not change the sketch
    for (int i = 0; i < n; i++) sketch_ptr->update(i);
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());
  }

  void serialize_deserialize_pinned_bytes() {
    cpc_sketch sketch(11);
    const int n(2000);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto data = sketch.serialize();
    auto sketch_ptr(cpc_sketch::deserialize(data.first.get(), data.second));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // updating again with the same values should not change the sketch
    for (int i = 0; i < n; i++) sketch_ptr->update(i);
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());
  }

  void serialize_deserialize_sliding_bytes() {
    cpc_sketch sketch(11);
    const int n(20000);
    for (int i = 0; i < n; i++) sketch.update(i);
    auto data = sketch.serialize();
    auto sketch_ptr(cpc_sketch::deserialize(data.first.get(), data.second));
    CPPUNIT_ASSERT_EQUAL(sketch.is_empty(), sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // updating again with the same values should not change the sketch
    for (int i = 0; i < n; i++) sketch_ptr->update(i);
    CPPUNIT_ASSERT_EQUAL(sketch.get_estimate(), sketch_ptr->get_estimate());
    CPPUNIT_ASSERT(sketch_ptr->validate());
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
    CPPUNIT_ASSERT(sketch_ptr->validate());

    // incompatible seed
    s.seekg(0); // rewind the stream to read the same sketch again
    CPPUNIT_ASSERT_THROW(cpc_sketch::deserialize(s), std::invalid_argument);
  }

  void kappa_range() {
    cpc_sketch s(11);
    CPPUNIT_ASSERT_EQUAL(0.0, s.get_lower_bound(1));
    CPPUNIT_ASSERT_EQUAL(0.0, s.get_upper_bound(1));
    CPPUNIT_ASSERT_EQUAL(0.0, s.get_lower_bound(2));
    CPPUNIT_ASSERT_EQUAL(0.0, s.get_upper_bound(2));
    CPPUNIT_ASSERT_EQUAL(0.0, s.get_lower_bound(3));
    CPPUNIT_ASSERT_EQUAL(0.0, s.get_upper_bound(3));
    CPPUNIT_ASSERT_THROW(s.get_lower_bound(4), std::invalid_argument);
    CPPUNIT_ASSERT_THROW(s.get_upper_bound(4), std::invalid_argument);
  }

  void validate_fail() {
    cpc_sketch sketch(11);
    const int n(2000);
    for (int i = 0; i < n; i++) sketch.update(i);
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    s.seekp(700); // the stream should be 856 bytes long. corrupt it somewhere before the end
    s << "corrupt data";
    auto sketch_ptr(cpc_sketch::deserialize(s));
    CPPUNIT_ASSERT(!sketch_ptr->validate());
  }

  void serialize_both_ways() {
    cpc_sketch sketch(11);
    const int n(2000);
    for (int i = 0; i < n; i++) sketch.update(i);
    const int header_size_bytes = 4;
    auto data(sketch.serialize(header_size_bytes));
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    CPPUNIT_ASSERT_EQUAL(data.second - header_size_bytes, (size_t) s.tellp());

    char* pp = new char[s.tellp()];
    s.read(pp, s.tellp());
    CPPUNIT_ASSERT(std::memcmp(pp, static_cast<char*>(data.first.get()) + header_size_bytes, data.second - header_size_bytes) == 0);
  }

  void update_int_equivalence() {
    cpc_sketch sketch(11);
    sketch.update((uint64_t) -1);
    sketch.update((int64_t) -1);
    sketch.update((uint32_t) -1);
    sketch.update((int32_t) -1);
    sketch.update((uint16_t) -1);
    sketch.update((int16_t) -1);
    sketch.update((uint8_t) -1);
    sketch.update((int8_t) -1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, sketch.get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
    std::ofstream os("cpc-negative-one.bin"); // to compare with Java
    sketch.serialize(os);
  }

  void update_float_equivalience() {
    cpc_sketch sketch(11);
    sketch.update((float) 1);
    sketch.update((double) 1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, sketch.get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
  }

  void update_string_equivalence() {
    cpc_sketch sketch(11);
    const std::string a("a");
    sketch.update(a);
    sketch.update(a.c_str(), a.length());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, sketch.get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(cpc_sketch_test);

} /* namespace datasketches */
