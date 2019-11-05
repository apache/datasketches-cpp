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

#include "cpc_union.hpp"

namespace datasketches {

static const double RELATIVE_ERROR_FOR_LG_K_11 = 0.02;

class cpc_union_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(cpc_union_test);
  CPPUNIT_TEST(lg_k_limits);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(copy);
  CPPUNIT_TEST(custom_seed);
  CPPUNIT_TEST(large);
  CPPUNIT_TEST(reduce_k_empty);
  CPPUNIT_TEST(reduce_k_sparse);
  CPPUNIT_TEST(reduce_k_window);
  CPPUNIT_TEST_SUITE_END();

  void lg_k_limits() {
    cpc_union u1(CPC_MIN_LG_K); // this should work
    cpc_union u2(CPC_MAX_LG_K); // this should work
    CPPUNIT_ASSERT_THROW(cpc_union u3(CPC_MIN_LG_K - 1), std::invalid_argument);
    CPPUNIT_ASSERT_THROW(cpc_union u4(CPC_MAX_LG_K + 1), std::invalid_argument);
  }

  void empty() {
    cpc_union u(11);
    auto s = u.get_result();
    CPPUNIT_ASSERT(s.is_empty());
    CPPUNIT_ASSERT_EQUAL(0.0, s.get_estimate());
  }

  void copy() {
    cpc_sketch s(11);
    s.update(1);
    cpc_union u1(11);
    u1.update(s);

    cpc_union u2 = u1; // copy constructor
    auto s1 = u2.get_result();
    CPPUNIT_ASSERT(!s1.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, s1.get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
    s.update(2);
    u2.update(s);
    u1 = u2; // operator=
    auto s2 = u1.get_result();
    CPPUNIT_ASSERT(!s2.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(2, s2.get_estimate(), 2 * RELATIVE_ERROR_FOR_LG_K_11);
  }

  void custom_seed() {
    cpc_sketch s(11, 123);

    s.update(1);
    s.update(2);
    s.update(3);

    cpc_union u1(11, 123);
    u1.update(s);
    auto r = u1.get_result();
    CPPUNIT_ASSERT(!r.is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(3, r.get_estimate(), 3 * RELATIVE_ERROR_FOR_LG_K_11);

    // incompatible seed
    cpc_union u2(11, 234);
    CPPUNIT_ASSERT_THROW(u2.update(s), std::invalid_argument);
  }

  void large() {
    int key = 0;
    cpc_sketch s(11);
    cpc_union u(11);
    for (int i = 0; i < 1000; i++) {
      cpc_sketch tmp(11);
      for (int i = 0; i < 10000; i++) {
        s.update(key);
        tmp.update(key);
        key++;
      }
      u.update(tmp);
    }
    cpc_sketch r = u.get_result();
    CPPUNIT_ASSERT_EQUAL(s.get_num_coupons(), r.get_num_coupons());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(s.get_estimate(), r.get_estimate(), s.get_estimate() * RELATIVE_ERROR_FOR_LG_K_11);
  }

  void reduce_k_empty() {
    cpc_sketch s(11);
    for (int i = 0; i < 10000; i++) s.update(i);
    cpc_union u(12);
    u.update(s);
    cpc_sketch r = u.get_result();
    CPPUNIT_ASSERT_EQUAL(11, (int) r.get_lg_k());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(10000, r.get_estimate(), 10000 * RELATIVE_ERROR_FOR_LG_K_11);
  }

  void reduce_k_sparse() {
    cpc_union u(12);

    cpc_sketch s12(12);
    for (int i = 0; i < 100; i++) s12.update(i);
    u.update(s12);

    cpc_sketch s11(11);
    for (int i = 0; i < 1000; i++) s11.update(i);
    u.update(s11);

    cpc_sketch r = u.get_result();
    CPPUNIT_ASSERT_EQUAL(11, (int) r.get_lg_k());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1000, r.get_estimate(), 1000 * RELATIVE_ERROR_FOR_LG_K_11);
  }

  void reduce_k_window() {
    cpc_union u(12);

    cpc_sketch s12(12);
    for (int i = 0; i < 500; i++) s12.update(i);
    u.update(s12);

    cpc_sketch s11(11);
    for (int i = 0; i < 1000; i++) s11.update(i);
    u.update(s11);

    cpc_sketch r = u.get_result();
    CPPUNIT_ASSERT_EQUAL(11, (int) r.get_lg_k());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1000, r.get_estimate(), 1000 * RELATIVE_ERROR_FOR_LG_K_11);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(cpc_union_test);

} /* namespace datasketches */
