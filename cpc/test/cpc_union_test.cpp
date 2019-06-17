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
  CPPUNIT_TEST_SUITE_END();

  void lg_k_limits() {
    cpc_union u1(CPC_MIN_LG_K); // this should work
    cpc_union u2(CPC_MAX_LG_K); // this should work
    CPPUNIT_ASSERT_THROW(cpc_union u3(CPC_MIN_LG_K - 1), std::invalid_argument);
    CPPUNIT_ASSERT_THROW(cpc_union u4(CPC_MAX_LG_K + 1), std::invalid_argument);
  }

  void empty() {
    cpc_union u(11);
    auto sketch_ptr(u.get_result());
    CPPUNIT_ASSERT(sketch_ptr->is_empty());
    CPPUNIT_ASSERT_EQUAL(0.0, sketch_ptr->get_estimate());
  }

  void copy() {
    cpc_sketch s(11);
    s.update(1);
    cpc_union u1(11);
    u1.update(s);

    cpc_union u2 = u1; // copy constructor
    auto sp1(u2.get_result());
    CPPUNIT_ASSERT(!sp1->is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(1, sp1->get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
    s.update(2);
    u2.update(s);
    u1 = u2; // operator=
    auto sp2(u1.get_result());
    CPPUNIT_ASSERT(!sp2->is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(2, sp2->get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);
  }

  void custom_seed() {
    cpc_sketch s(11, 123);

    s.update(1);
    s.update(2);
    s.update(3);

    cpc_union u1(11, 123);
    u1.update(s);
    auto sp(u1.get_result());
    CPPUNIT_ASSERT(!sp->is_empty());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(3, sp->get_estimate(), RELATIVE_ERROR_FOR_LG_K_11);

    // incompatible seed
    cpc_union u2(11, 234);
    CPPUNIT_ASSERT_THROW(u2.update(s), std::invalid_argument);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(cpc_union_test);

} /* namespace datasketches */
