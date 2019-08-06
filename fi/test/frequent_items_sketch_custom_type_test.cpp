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

#include <frequent_items_sketch.hpp>
#include "../../common/test/test_type.hpp"

namespace datasketches {

typedef frequent_items_sketch<test_type, test_type_hash, test_type_equal, test_type_serde> frequent_test_type_sketch;

class frequent_items_sketch_custom_type_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(frequent_items_sketch_custom_type_test);
  CPPUNIT_TEST(custom_type);
  CPPUNIT_TEST_SUITE_END();

  void custom_type() {

    frequent_test_type_sketch sketch(3);
    sketch.update(1, 10); // should survive the purge
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7);
    test_type a8(8);
    sketch.update(a8);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(17, (int) sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(10, (int) sketch.get_estimate(1));
    std::cerr << "num active: " << sketch.get_num_active_items() << std::endl;

    std::cerr << "get frequent items" << std::endl;
    auto items = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_POSITIVES);
    CPPUNIT_ASSERT_EQUAL(1, (int) items.size()); // only 1 item should be above threshold
    CPPUNIT_ASSERT_EQUAL(1, items[0].get_item().get_value());
    CPPUNIT_ASSERT_EQUAL(10, (int) items[0].get_estimate());

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    std::cerr << "serialize" << std::endl;
    sketch.serialize(s);
    std::cerr << "deserialize" << std::endl;
    auto sketch2 = frequent_test_type_sketch::deserialize(s);
    CPPUNIT_ASSERT(!sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(17, (int) sketch2.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(10, (int) sketch2.get_estimate(1));
    CPPUNIT_ASSERT_EQUAL(sketch.get_num_active_items(), sketch2.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(sketch.get_maximum_error(), sketch2.get_maximum_error());
    std::cerr << "end" << std::endl;

    sketch2.to_stream(std::cerr, true);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(frequent_items_sketch_custom_type_test);

} /* namespace datasketches */
