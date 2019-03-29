/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <frequent_items_sketch.hpp>

namespace datasketches {

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

struct serdeA {
  void serialize(std::ostream& os, const A* items, unsigned num) {
    for (unsigned i = 0; i < num; i++) {
      const int value = items[i].get_value();
      os.write((char*)&value, sizeof(value));
    }
  }
  void deserialize(std::istream& is, A* items, unsigned num) {
    for (unsigned i = 0; i < num; i++) {
      int value;
      is.read((char*)&value, sizeof(value));
      new (&items[i]) A(value);
    }
  }
};

std::ostream& operator<<(std::ostream& os, const A& a) {
  os << a.get_value();
  return os;
}

typedef frequent_items_sketch<A, hashA, equalA, serdeA> frequent_A_sketch;

class frequent_items_sketch_custom_type_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(frequent_items_sketch_custom_type_test);
  CPPUNIT_TEST(custom_type);
  CPPUNIT_TEST_SUITE_END();

  void custom_type() {

    frequent_A_sketch sketch(3);
    sketch.update(1, 10); // should survive the purge
    sketch.update(2);
    sketch.update(3);
    sketch.update(4);
    sketch.update(5);
    sketch.update(6);
    sketch.update(7);
    A a8(8);
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
    auto sketch2 = frequent_A_sketch::deserialize(s);
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
