/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <reverse_purge_hash_map.hpp>

namespace datasketches {

class reverse_purge_hash_map_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(reverse_purge_hash_map_test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(one_item);
  CPPUNIT_TEST(iterator);
  CPPUNIT_TEST_SUITE_END();

  void empty() {
    reverse_purge_hash_map<int> map(3, 3);
    CPPUNIT_ASSERT_EQUAL(0U, map.get_num_active());
    CPPUNIT_ASSERT_EQUAL(static_cast<uint8_t>(3), map.get_lg_cur_size());
  }

  void one_item() {
    reverse_purge_hash_map<int> map(3, 3);
    map.adjust_or_insert(1, 1);
    CPPUNIT_ASSERT_EQUAL(1U, map.get_num_active());
    CPPUNIT_ASSERT_EQUAL(1ULL, map.get(1));
  }

  void iterator() {
    reverse_purge_hash_map<int> map(3, 4);
    for (int i = 0; i < 11; i++) map.adjust_or_insert(i, 1); // this should fit with no purge
    int sum = 0;
    for (auto &it: map) sum += it.second;
    CPPUNIT_ASSERT_EQUAL(11, sum);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(reverse_purge_hash_map_test);

} /* namespace datasketches */
