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

#include "AuxHashMap.hpp"

#include <memory>
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
    AuxHashMap<>* map = new AuxHashMap<>(3, 7);
    map->mustAdd(100, 5);
    int val = map->mustFindValueFor(100);
    CPPUNIT_ASSERT_EQUAL(val, 5);

    map->mustReplace(100, 10);
    val = map->mustFindValueFor(100);
    CPPUNIT_ASSERT_EQUAL(val, 10);

    CPPUNIT_ASSERT_THROW_MESSAGE("map->mustReplace() should fail",
                                 map->mustReplace(101, 5), std::invalid_argument);

    delete map;
  }

  void checkGrowSpace() {
    auto map = std::unique_ptr<AuxHashMap<>, std::function<void(AuxHashMap<>*)>>(
        AuxHashMap<>::newAuxHashMap(3, 7),
        AuxHashMap<>::make_deleter()
        );
    CPPUNIT_ASSERT_EQUAL(map->getLgAuxArrInts(), 3);
    for (int i = 1; i <= 7; ++i) {
      map->mustAdd(i, i);
    }
    CPPUNIT_ASSERT_EQUAL(map->getLgAuxArrInts(), 4);
    std::unique_ptr<PairIterator<>, std::function<void(PairIterator<>*)>> itr = map->getIterator();
    int count1 = 0;
    int count2 = 0;
    while (itr->nextAll()) {
      ++count2;
      int pair = itr->getPair();
      if (pair != 0) { ++count1; }
    }
    CPPUNIT_ASSERT_EQUAL(count1, 7);
    CPPUNIT_ASSERT_EQUAL(count2, 16);
  }

  void checkExceptionMustFindValueFor() {
    AuxHashMap<> map(3, 7);
    map.mustAdd(100, 5);
    CPPUNIT_ASSERT_THROW_MESSAGE("map.mustFindValueFor() should fail",
                                 map.mustFindValueFor(101), std::invalid_argument);
  }

  void checkExceptionMustAdd() {
    AuxHashMap<>* map = AuxHashMap<>::newAuxHashMap(3, 7);
    map->mustAdd(100, 5);
    CPPUNIT_ASSERT_THROW_MESSAGE("map->mustAdd() should fail",
                                 map->mustAdd(100, 6), std::invalid_argument);
    
    AuxHashMap<>::make_deleter()(map);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(AuxHashMapTest);

} /* namespace datasketches */
