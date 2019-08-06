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

#ifndef CLASS_TEST_TYPE_HPP_
#define CLASS_TEST_TYPE_HPP_

#include <iostream>

namespace datasketches {

class test_type {
  static const bool DEBUG = false;
public:
  // no default constructor should be required
  test_type(int value): value(value) {
    if (DEBUG) std::cerr << "A constructor" << std::endl;
  }
  ~test_type() {
    if (DEBUG) std::cerr << "A destructor" << std::endl;
  }
  test_type(const test_type& other): value(other.value) {
    if (DEBUG) std::cerr << "A copy constructor" << std::endl;
  }
  // noexcept is important here so that, for instance, std::vector could move this type
  test_type(test_type&& other) noexcept : value(other.value) {
    if (DEBUG) std::cerr << "A move constructor" << std::endl;
  }
  test_type& operator=(const test_type& other) {
    if (DEBUG) std::cerr << "A copy assignment" << std::endl;
    value = other.value;
    return *this;
  }
  test_type& operator=(test_type&& other) {
    if (DEBUG) std::cerr << "A move assignment" << std::endl;
    value = other.value;
    return *this;
  }
  int get_value() const { return value; }
private:
  int value;
};

struct test_type_hash {
  std::size_t operator()(const test_type& a) const {
    return std::hash<int>()(a.get_value());
  }
};

struct test_type_equal {
  bool operator()(const test_type& a1, const test_type& a2) const {
    return a1.get_value() == a2.get_value();
  }
};

struct test_type_less {
  bool operator()(const test_type& a1, const test_type& a2) const {
    return a1.get_value() < a2.get_value();
  }
};

struct test_type_serde {
  void serialize(std::ostream& os, const test_type* items, unsigned num) {
    for (unsigned i = 0; i < num; i++) {
      const int value = items[i].get_value();
      os.write((char*)&value, sizeof(value));
    }
  }
  void deserialize(std::istream& is, test_type* items, unsigned num) {
    for (unsigned i = 0; i < num; i++) {
      int value;
      is.read((char*)&value, sizeof(value));
      new (&items[i]) test_type(value);
    }
  }
  size_t size_of_item(const test_type& item) {
    return sizeof(int);
  }
};

std::ostream& operator<<(std::ostream& os, const test_type& a) {
  os << a.get_value();
  return os;
}

} /* namespace datasketches */

#endif
