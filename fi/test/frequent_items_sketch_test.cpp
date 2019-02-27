/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <frequent_items_sketch.hpp>

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
  CPPUNIT_TEST(custom_type);
  CPPUNIT_TEST_SUITE_END();

  void empty() {
    frequent_items_sketch<int> sketch(3);
    CPPUNIT_ASSERT(sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(0U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(0ULL, sketch.get_total_weight());
  }

  void one_item() {
    frequent_items_sketch<std::string> sketch(3);
    sketch.update("a");
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL(1U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch.get_estimate("a"));
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch.get_lower_bound("a"));
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch.get_upper_bound("a"));
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
    CPPUNIT_ASSERT_EQUAL(7ULL, sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(4U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch.get_estimate("a"));
    CPPUNIT_ASSERT_EQUAL(3ULL, sketch.get_estimate("b"));
    CPPUNIT_ASSERT_EQUAL(2ULL, sketch.get_estimate("c"));
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch.get_estimate("d"));
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
    CPPUNIT_ASSERT_EQUAL(15ULL, sketch.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(12U, sketch.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch.get_estimate("a"));
    CPPUNIT_ASSERT_EQUAL(3ULL, sketch.get_estimate("b"));
    CPPUNIT_ASSERT_EQUAL(2ULL, sketch.get_estimate("c"));
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch.get_estimate("d"));
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
    CPPUNIT_ASSERT_EQUAL(35ULL, sketch.get_total_weight());

    auto items = sketch.get_frequent_items(frequent_items_sketch<int>::error_type::NO_FALSE_POSITIVES);
    CPPUNIT_ASSERT_EQUAL(2, (int) items.size()); // only 2 items (1 and 7) should have counts more than 1
    CPPUNIT_ASSERT_EQUAL(7, items[0].get_item());
    CPPUNIT_ASSERT_EQUAL(15ULL, items[0].get_estimate());
    CPPUNIT_ASSERT_EQUAL(1, items[1].get_item());
    CPPUNIT_ASSERT_EQUAL(10ULL, items[1].get_estimate());

    items = sketch.get_frequent_items(frequent_items_sketch<int>::error_type::NO_FALSE_NEGATIVES);
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
    CPPUNIT_ASSERT_EQUAL(7ULL, sketch1.get_total_weight());
    CPPUNIT_ASSERT_EQUAL(4U, sketch1.get_num_active_items());
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch1.get_estimate(1));
    CPPUNIT_ASSERT_EQUAL(3ULL, sketch1.get_estimate(2));
    CPPUNIT_ASSERT_EQUAL(2ULL, sketch1.get_estimate(3));
    CPPUNIT_ASSERT_EQUAL(1ULL, sketch1.get_estimate(4));
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
    CPPUNIT_ASSERT_EQUAL(46ULL, sketch1.get_total_weight());
    CPPUNIT_ASSERT(2U <= sketch1.get_num_active_items());

    auto items = sketch1.get_frequent_items(2, frequent_items_sketch<int>::error_type::NO_FALSE_POSITIVES);
    CPPUNIT_ASSERT_EQUAL(2, (int) items.size()); // only 2 items (1 and 21) should be above threshold
    CPPUNIT_ASSERT_EQUAL(21, items[0].get_item());
    CPPUNIT_ASSERT(11ULL <= items[0].get_estimate()); // always overestimated
    CPPUNIT_ASSERT_EQUAL(1, items[1].get_item());
    CPPUNIT_ASSERT(9ULL <= items[1].get_estimate()); // always overestimated
  }

  void custom_type() {
    class A {
    public:
      A(int value): value(value) {
        std::cerr << "A constructor" << std::endl;
      }
      ~A() {
        std::cerr << "A destructor" << std::endl;
      }
      A(const A& other): value(other.value) {
        std::cerr << "A copy constructor" << std::endl;
      }
      // noexcept is important here so that std::vector can move this type
      A(A&& other) noexcept : value(other.value) {
        std::cerr << "A move constructor" << std::endl;
      }
      A& operator=(const A& other) {
        std::cerr << "A copy assignment" << std::endl;
        value = other.value;
        return *this;
      }
      A& operator=(A&& other) {
        std::cerr << "A move assignment" << std::endl;
        value = other.value;
        return *this;
      }
      int get_value() const { return value; }
    private:
      int value;
    };

    struct hashA {
      std::size_t operator()(const A& a) const {
        return std::hash<int>()(a.get_value());
      }
    };

    struct equalA {
      bool operator()(const A& a1, const A& a2) const {
        return a1.get_value() == a2.get_value();
      }
    };

    frequent_items_sketch<A, hashA, equalA> sketch(3);
    sketch.update(1, 10); // should survive the purge
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7);
    A a8(8);
    sketch.update(a8);

    auto items = sketch.get_frequent_items(frequent_items_sketch<A, hashA, equalA>::error_type::NO_FALSE_POSITIVES);
    CPPUNIT_ASSERT_EQUAL(1, (int) items.size()); // only 1 item should be above threshold
    CPPUNIT_ASSERT_EQUAL(1, items[0].get_item().get_value());
    CPPUNIT_ASSERT_EQUAL(10ULL, items[0].get_estimate());
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(frequent_items_sketch_test);

} /* namespace datasketches */
