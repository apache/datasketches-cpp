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

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

class frequent_items_sketch_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(frequent_items_sketch_test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(one_item);
  CPPUNIT_TEST(several_items_no_resize_no_purge);
  CPPUNIT_TEST(several_items_with_resize_no_purge);
  CPPUNIT_TEST(estimation_mode);
  CPPUNIT_TEST(merge_exact_mode);
  CPPUNIT_TEST(merge_estimation_mode);
  CPPUNIT_TEST(deserialize_from_java_long);
  CPPUNIT_TEST(deserialize_from_java_string);
  CPPUNIT_TEST(deserialize_from_java_string_utf8);
  CPPUNIT_TEST(serialize_deserialize_long64_stream);
  CPPUNIT_TEST(serialize_deserialize_long64_bytes);
  CPPUNIT_TEST(serialize_deserialize_string_stream);
  CPPUNIT_TEST(serialize_deserialize_string_bytes);
  CPPUNIT_TEST(serialize_deserialize_string_utf8_stream);
  CPPUNIT_TEST_SUITE_END();

  void empty() {
    frequent_items_sketch<int> sketch(3);
    CPPUNIT_ASSERT(sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(0U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(0, (int) sketch.get_total_weight());
  }

  void one_item() {
    frequent_items_sketch<std::string> sketch(3);
    sketch.update("a");
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(1U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("a"));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_lower_bound("a"));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_upper_bound("a"));
  }

  void several_items_no_resize_no_purge() {
    frequent_items_sketch<std::string> sketch(3);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    sketch.update("d");
    sketch.update("b");
    sketch.update("c");
    sketch.update("b");
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(7, (int) sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(4U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("a"));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch.get_estimate("b"));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch.get_estimate("c"));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("d"));
  }

  void several_items_with_resize_no_purge() {
    frequent_items_sketch<std::string> sketch(4);
    sketch.update("a");
    sketch.update("b");
    sketch.update("c");
    sketch.update("d");
    sketch.update("b");
    sketch.update("c");
    sketch.update("b");
    sketch.update("e");
    sketch.update("f");
    sketch.update("g");
    sketch.update("h");
    sketch.update("i");
    sketch.update("j");
    sketch.update("k");
    sketch.update("l");
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(15, (int) sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(12U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("a"));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch.get_estimate("b"));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch.get_estimate("c"));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("d"));
  }

  void estimation_mode() {
    frequent_items_sketch<int> sketch(3);
    sketch.update(1, 10);
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7, 15);
    sketch.update(8);
    sketch.update(9);
    sketch.update(10);
    sketch.update(11);
    sketch.update(12);
    CPPUNIT_ASSERT(sketch.get_maximum_error() > 0); // estimation mode

    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(35, (int) sketch.get_total_weight());

    auto items = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_POSITIVES);
    CPPUNIT_ASSERT_EQUAL(2, (int) items.size()); // only 2 items (1 and 7) should have counts more than 1
    CPPUNIT_ASSERT_EQUAL(7, items[0].get_item());
    CPPUNIT_ASSERT_EQUAL(15, (int) items[0].get_estimate());
    CPPUNIT_ASSERT_EQUAL(1, items[1].get_item());
    CPPUNIT_ASSERT_EQUAL(10, (int) items[1].get_estimate());

    items = sketch.get_frequent_items(frequent_items_error_type::NO_FALSE_NEGATIVES);
    CPPUNIT_ASSERT(2 <= (int) items.size()); // at least 2 items
    CPPUNIT_ASSERT(12 >= (int) items.size()); // but not more than 12 items
  }

  void merge_exact_mode() {
    frequent_items_sketch<int> sketch1(3);
    sketch1.update(1);
    sketch1.update(2);
    sketch1.update(3);
    sketch1.update(4);

    frequent_items_sketch<int> sketch2(3);
    sketch1.update(2);
    sketch1.update(3);
    sketch1.update(2);

    sketch1.merge(sketch2);
    CPPUNIT_ASSERT(!sketch1.is_empty());
    CPPUNIT_ASSERT_EQUAL(7, (int) sketch1.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(4U, sketch1.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch1.get_estimate(1));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch1.get_estimate(2));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch1.get_estimate(3));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch1.get_estimate(4));
  }

  void merge_estimation_mode() {
    frequent_items_sketch<int> sketch1(4);
    sketch1.update(1, 9); // to make sure it survives the purge
    sketch1.update(2);
    sketch1.update(3);
    sketch1.update(4);
    sketch1.update(5);
    sketch1.update(6);
    sketch1.update(7);
    sketch1.update(8);
    sketch1.update(9);
    sketch1.update(10);
    sketch1.update(11);
    sketch1.update(12);
    sketch1.update(13);
    sketch1.update(14);
    CPPUNIT_ASSERT(sketch1.get_maximum_error() > 0); // estimation mode

    frequent_items_sketch<int> sketch2(4);
    sketch2.update(8);
    sketch2.update(9);
    sketch2.update(10);
    sketch2.update(11);
    sketch2.update(12);
    sketch2.update(13);
    sketch2.update(14);
    sketch2.update(15);
    sketch2.update(16);
    sketch2.update(17);
    sketch2.update(18);
    sketch2.update(19);
    sketch2.update(20);
    sketch2.update(21, 11); // to make sure it survives the purge
    CPPUNIT_ASSERT(sketch2.get_maximum_error() > 0); // estimation mode

    sketch1.merge(sketch2);
    CPPUNIT_ASSERT(!sketch1.is_empty());
    CPPUNIT_ASSERT_EQUAL(46, (int) sketch1.get_total_weight());
    CPPUNIT_ASSERT(2U <= sketch1.get_num_active_items());

    auto items = sketch1.get_frequent_items(frequent_items_error_type::NO_FALSE_POSITIVES, 2);
    CPPUNIT_ASSERT_EQUAL(2, (int) items.size()); // only 2 items (1 and 21) should be above threshold
    CPPUNIT_ASSERT_EQUAL(21, items[0].get_item());
    CPPUNIT_ASSERT(11 <= (int) items[0].get_estimate()); // always overestimated
    CPPUNIT_ASSERT_EQUAL(1, items[1].get_item());
    CPPUNIT_ASSERT(9 <= (int) items[1].get_estimate()); // always overestimated
  }

  void deserialize_from_java_long() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "longs_sketch_from_java.bin", std::ios::binary);
    auto sketch = frequent_items_sketch<long long>::deserialize(is);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(4, (int) sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(4U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate(1));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate(2));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate(3));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate(4));
  }

  void deserialize_from_java_string() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "items_sketch_string_from_java.bin", std::ios::binary);
    auto sketch = frequent_items_sketch<std::string>::deserialize(is);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(4, (int) sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(4U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("aaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("bbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("ccccccccccccccccccccccccccccc"));
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("ddddddddddddddddddddddddddddd"));
  }

  void deserialize_from_java_string_utf8() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "items_sketch_string_utf8_from_java.bin", std::ios::binary);
    auto sketch = frequent_items_sketch<std::string>::deserialize(is);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(10, (int) sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(4U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch.get_estimate("абвгд"));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch.get_estimate("еёжзи"));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch.get_estimate("йклмн"));
    CPPUNIT_ASSERT_EQUAL(4, (int) sketch.get_estimate("опрст"));
  }

  void serialize_deserialize_long64_stream() {
    frequent_items_sketch<long long> sketch1(3);
    sketch1.update(1, 1);
    sketch1.update(2, 2);
    sketch1.update(3, 3);
    sketch1.update(4, 4);
    sketch1.update(5, 5);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch1.serialize(s);
    auto sketch2 = frequent_items_sketch<long long>::deserialize(s);
    CPPUNIT_ASSERT(!sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(15, (int) sketch2.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(5U, sketch2.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch2.get_estimate(1));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch2.get_estimate(2));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch2.get_estimate(3));
    CPPUNIT_ASSERT_EQUAL(4, (int) sketch2.get_estimate(4));
    CPPUNIT_ASSERT_EQUAL(5, (int) sketch2.get_estimate(5));
  }

  void serialize_deserialize_long64_bytes() {
    frequent_items_sketch<long long> sketch1(3);
    sketch1.update(1, 1);
    sketch1.update(2, 2);
    sketch1.update(3, 3);
    sketch1.update(4, 4);
    sketch1.update(5, 5);

    auto p = sketch1.serialize();
    auto sketch2 = frequent_items_sketch<long long>::deserialize(p.first.get(), p.second);
    CPPUNIT_ASSERT(!sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(15, (int) sketch2.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(5U, sketch2.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch2.get_estimate(1));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch2.get_estimate(2));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch2.get_estimate(3));
    CPPUNIT_ASSERT_EQUAL(4, (int) sketch2.get_estimate(4));
    CPPUNIT_ASSERT_EQUAL(5, (int) sketch2.get_estimate(5));
  }

  void serialize_deserialize_string_stream() {
    frequent_items_sketch<std::string> sketch1(3);
    sketch1.update("aaaaaaaaaaaaaaaa", 1);
    sketch1.update("bbbbbbbbbbbbbbbb", 2);
    sketch1.update("cccccccccccccccc", 3);
    sketch1.update("dddddddddddddddd", 4);
    sketch1.update("eeeeeeeeeeeeeeee", 5);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch1.serialize(s);
    auto sketch2 = frequent_items_sketch<std::string>::deserialize(s);
    CPPUNIT_ASSERT(!sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(15, (int) sketch2.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(5U, sketch2.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch2.get_estimate("aaaaaaaaaaaaaaaa"));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch2.get_estimate("bbbbbbbbbbbbbbbb"));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch2.get_estimate("cccccccccccccccc"));
    CPPUNIT_ASSERT_EQUAL(4, (int) sketch2.get_estimate("dddddddddddddddd"));
    CPPUNIT_ASSERT_EQUAL(5, (int) sketch2.get_estimate("eeeeeeeeeeeeeeee"));
  }

  void serialize_deserialize_string_bytes() {
    frequent_items_sketch<std::string> sketch1(3);
    sketch1.update("aaaaaaaaaaaaaaaa", 1);
    sketch1.update("bbbbbbbbbbbbbbbb", 2);
    sketch1.update("cccccccccccccccc", 3);
    sketch1.update("dddddddddddddddd", 4);
    sketch1.update("eeeeeeeeeeeeeeee", 5);

    auto p = sketch1.serialize();
    auto sketch2 = frequent_items_sketch<std::string>::deserialize(p.first.get(), p.second);
    CPPUNIT_ASSERT(!sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(15, (int) sketch2.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(5U, sketch2.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch2.get_estimate("aaaaaaaaaaaaaaaa"));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch2.get_estimate("bbbbbbbbbbbbbbbb"));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch2.get_estimate("cccccccccccccccc"));
    CPPUNIT_ASSERT_EQUAL(4, (int) sketch2.get_estimate("dddddddddddddddd"));
    CPPUNIT_ASSERT_EQUAL(5, (int) sketch2.get_estimate("eeeeeeeeeeeeeeee"));
  }

  void serialize_deserialize_string_utf8_stream() {
    frequent_items_sketch<std::string> sketch1(3);
    sketch1.update("абвгд", 1);
    sketch1.update("еёжзи", 2);
    sketch1.update("йклмн", 3);
    sketch1.update("опрст", 4);
    sketch1.update("уфхцч", 5);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch1.serialize(s);
    auto sketch2 = frequent_items_sketch<std::string>::deserialize(s);
    CPPUNIT_ASSERT(!sketch2.is_empty());
    CPPUNIT_ASSERT_EQUAL(15, (int) sketch2.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(5U, sketch2.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1, (int) sketch2.get_estimate("абвгд"));
    CPPUNIT_ASSERT_EQUAL(2, (int) sketch2.get_estimate("еёжзи"));
    CPPUNIT_ASSERT_EQUAL(3, (int) sketch2.get_estimate("йклмн"));
    CPPUNIT_ASSERT_EQUAL(4, (int) sketch2.get_estimate("опрст"));
    CPPUNIT_ASSERT_EQUAL(5, (int) sketch2.get_estimate("уфхцч"));
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(frequent_items_sketch_test);

} /* namespace datasketches */
