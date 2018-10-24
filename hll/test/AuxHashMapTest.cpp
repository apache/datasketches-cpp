/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "hll.hpp"
#include "CouponList.hpp"
#include "CouponHashSet.hpp"
#include "HllArray.hpp"

#include "HllSketch.hpp"
#include "HllUnion.hpp"
#include "HllUtil.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class AuxHashMapTest : public CppUnit::TestFixture {

  // list of values defined at bottom of file
  static const int nArr[]; // = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

  CPPUNIT_TEST_SUITE(AuxHashMapTest);
  CPPUNIT_TEST(checkMustReplace);
  CPPUNIT_TEST(checkGrowSpace);
  CPPUNIT_TEST(checkExceptionMustFindValueFor);
  CPPUNIT_TEST(checkExceptionMustAdd);
  CPPUNIT_TEST_SUITE_END();

  void checkMustReplace() {
    AuxHashMap* map = new AuxHashMap(3, 7);
    map->mustAdd(100, 5);
    int val = map->mustFindValueFor(100);
    CPPUNIT_ASSERT_EQUAL(val, 5);

    map->mustReplace(100, 10);
    val = map->mustFindValueFor(100);
    CPPUNIT_ASSERT_EQUAL(val, 10);

    try {
      map->mustReplace(101, 5);
      CPPUNIT_FAIL("map->mustReplace() should fail");
    } catch (std::exception& e) {
      // expected
    }

    delete map;
  }

  void checkGrowSpace() {
    AuxHashMap* map = new AuxHashMap(3, 7);
    CPPUNIT_ASSERT_EQUAL(map->getLgAuxArrInts(), 3);
    for (int i = 1; i <= 7; ++i) {
      map->mustAdd(i, i);
    }
    CPPUNIT_ASSERT_EQUAL(map->getLgAuxArrInts(), 4);
    std::unique_ptr<PairIterator> itr = map->getIterator();
    int count1 = 0;
    int count2 = 0;
    while (itr->nextAll()) {
      ++count2;
      int pair = itr->getPair();
      if (pair != 0) { ++count1; }
    }
    CPPUNIT_ASSERT_EQUAL(count1, 7);
    CPPUNIT_ASSERT_EQUAL(count2, 16);

    delete map;
  }

  void checkExceptionMustFindValueFor() {
    AuxHashMap* map = new AuxHashMap(3, 7);
    try {
      map->mustAdd(100, 5);
      map->mustFindValueFor(101);
      CPPUNIT_FAIL("map->mustFindValueFor() should fail");
    } catch (std::exception& e) {
      // expected
    }
    delete map;
  }

  void checkExceptionMustAdd() {
    AuxHashMap* map = new AuxHashMap(3, 7);
    try {
      map->mustAdd(100, 5);
      map->mustAdd(100, 6);
      CPPUNIT_FAIL("map->mustAdd() should fail");
    } catch (std::exception& e) {
      // expected
    }
    delete map;
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(AuxHashMapTest);

} /* namespace datasketches */
