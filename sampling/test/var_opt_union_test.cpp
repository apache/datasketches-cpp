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

#include <var_opt_sketch.hpp>
#include <var_opt_union.hpp>

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <cmath>
#include <random>

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

class var_opt_union_test: public CppUnit::TestFixture {

  static constexpr double EPS = 1e-13;
  CPPUNIT_TEST_SUITE(var_opt_union_test);
  CPPUNIT_TEST(bad_prelongs);
  CPPUNIT_TEST(bad_ser_ver);
  CPPUNIT_TEST(bad_family);
  CPPUNIT_TEST(invalid_k);
  CPPUNIT_TEST(empty_union);
  CPPUNIT_TEST(two_exact_sketches);
  CPPUNIT_TEST(heavy_sampling_sketch);
  CPPUNIT_TEST(identical_sampling_sketches);
  CPPUNIT_TEST(small_sampling_sketch);
  CPPUNIT_TEST(serialize_empty);
  CPPUNIT_TEST(serialize_exact);
  CPPUNIT_TEST(serialize_sampling);
  // CPPUNIT_TEST(deserialize_exact_from_java);
  // CPPUNIT_TEST(deserialize_sampling_from_java);
  CPPUNIT_TEST_SUITE_END();

  var_opt_sketch<int> create_unweighted_sketch(uint32_t k, uint64_t n) {
    var_opt_sketch<int> sk(k);
    for (uint64_t i = 0; i < n; ++i) {
      sk.update(i, 1.0);
    }
    return sk;
  }

  // if exact_compare = false, checks for equivalence -- specific R region values may differ but
  // R region weights must match
  template<typename T, typename S, typename A>
  void check_if_equal(var_opt_sketch<T,S,A>& sk1, var_opt_sketch<T,S,A>& sk2, bool exact_compare = true) {
    CPPUNIT_ASSERT_EQUAL_MESSAGE("sketches have different values of k",
      sk1.get_k(), sk2.get_k());
    CPPUNIT_ASSERT_EQUAL_MESSAGE("sketches have different values of n",
      sk1.get_n(), sk2.get_n());
    CPPUNIT_ASSERT_EQUAL_MESSAGE("sketches have different sample counts",
      sk1.get_num_samples(), sk2.get_num_samples());
      
    auto it1 = sk1.begin();
    auto it2 = sk2.begin();
    size_t i = 0;

    while ((it1 != sk1.end()) && (it2 != sk2.end())) {
      const std::pair<const T&, const double> p1 = *it1;
      const std::pair<const T&, const double> p2 = *it2;
      if (exact_compare) {
        CPPUNIT_ASSERT_EQUAL_MESSAGE("data values differ at sample " + std::to_string(i),
          p1.first, p2.first); 
      }
      CPPUNIT_ASSERT_EQUAL_MESSAGE("weight values differ at sample " + std::to_string(i),
        p1.second, p2.second);
      ++i;
      ++it1;
      ++it2;
    }

    CPPUNIT_ASSERT_MESSAGE("iterators did not end at the same time",
      (it1 == sk1.end()) && (it2 == sk2.end()));
  }

  // compare serialization and deserializationi results, crossing methods to ensure that
  // the resulting binary images are compatible.
  // if exact_compare = false, checks for equivalence -- specific R region values may differ but
  // R region weights must match
  template<typename T, typename S, typename A>
  void compare_serialization_deserialization(var_opt_union<T,S,A>& vo_union, bool exact_compare = true) {
    std::vector<uint8_t> bytes = vo_union.serialize();

    var_opt_union<T> u_from_bytes = var_opt_union<T>::deserialize(bytes.data(), bytes.size());
    var_opt_sketch<T> sk1 = vo_union.get_result();
    var_opt_sketch<T> sk2 = u_from_bytes.get_result();
    check_if_equal(sk1, sk2, exact_compare);

    std::string str(bytes.begin(), bytes.end());
    std::stringstream ss;
    ss.str(str);

    var_opt_union<T> u_from_stream = var_opt_union<T>::deserialize(ss);
    sk2 = u_from_stream.get_result();
    check_if_equal(sk1, sk2, exact_compare);

    ss.seekg(0); // didn't put anything so only reset read position
    vo_union.serialize(ss);
    u_from_stream = var_opt_union<T>::deserialize(ss);
    sk2 = u_from_stream.get_result();
    check_if_equal(sk1, sk2, exact_compare);

    std::string str_from_stream = ss.str();
    var_opt_union<T> u_from_str = var_opt_union<T>::deserialize(str_from_stream.c_str(), str_from_stream.size());
    sk2 = u_from_str.get_result();
    check_if_equal(sk1, sk2, exact_compare);
  }

  void bad_prelongs() {
    var_opt_sketch<int> sk = create_unweighted_sketch(32, 33);
    var_opt_union<int> u(32);
    u.update(sk);
    std::vector<uint8_t> bytes = u.serialize();

    bytes[0] = 0; // corrupt the preamble longs byte to be too small
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch bad preamble longs",
      var_opt_union<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    // create a stringstream to check the same
    std::stringstream ss;
    std::string str(bytes.begin(), bytes.end());
    ss.str(str);
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(stream) failed to catch bad serialization version",
      var_opt_union<int>::deserialize(ss),
      std::invalid_argument);
  }

  void bad_ser_ver() {
    var_opt_sketch<int> sk = create_unweighted_sketch(16, 16);
    var_opt_union<int> u(32);
    u.update(sk);
    std::vector<uint8_t> bytes = u.serialize();
    bytes[1] = 0; // corrupt the serialization version byte

    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch bad serialization version",
      var_opt_union<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    // create a stringstream to check the same
    std::stringstream ss;
    std::string str(bytes.begin(), bytes.end());
    ss.str(str);
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(stream) failed to catch bad serialization version",
      var_opt_union<int>::deserialize(ss),
      std::invalid_argument);
  }

  void invalid_k() {
    CPPUNIT_ASSERT_THROW_MESSAGE("constructor failed to catch invalid k = 0",
      var_opt_union<int> sk(0),
      std::invalid_argument);

    CPPUNIT_ASSERT_THROW_MESSAGE("constructor failed to catch invalid k < 0 (aka >= 2^31)",
      var_opt_union<std::string> sk(1<<31),
      std::invalid_argument);
  }
  
  void bad_family() {
    var_opt_sketch<int> sk = create_unweighted_sketch(16, 16);
    var_opt_union<int> u(15);
    u.update(sk);
    std::vector<uint8_t> bytes = u.serialize();
    bytes[2] = 0; // corrupt the family byte

    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch bad family id",
      var_opt_union<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    std::stringstream ss;
    std::string str(bytes.begin(), bytes.end());
    ss.str(str);
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(stream) failed to catch bad family id",
      var_opt_union<int>::deserialize(ss),
      std::invalid_argument);
  }

  void empty_union() {
    uint32_t k = 2048;
    var_opt_sketch<std::string> sk(k);
    var_opt_union<std::string> u(k);
    u.update(sk);

    var_opt_sketch<std::string> result = u.get_result();
    CPPUNIT_ASSERT(result.is_empty());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 0, result.get_n());
    CPPUNIT_ASSERT_EQUAL((uint32_t) 0, result.get_num_samples());
    CPPUNIT_ASSERT_EQUAL(k, result.get_k());
  }

  void two_exact_sketches() {
    uint64_t n = 4; // 2n < k
    uint32_t k = 10;
    var_opt_sketch<int> sk1(k), sk2(k);

    for (int i = 1; i <= n; ++i) {
      sk1.update(i, i);
      sk2.update(-i, i);
    }

    var_opt_union<int> u(k);
    u.update(sk1);
    u.update(sk2);

    var_opt_sketch<int> result = u.get_result();
    CPPUNIT_ASSERT_EQUAL(2 * n, result.get_n());
    CPPUNIT_ASSERT_EQUAL(k, result.get_k());
  }

  void heavy_sampling_sketch() {
    uint64_t n1 = 20;
    uint32_t k1 = 10;
    uint64_t n2 = 6;
    uint32_t k2 = 5;
    var_opt_sketch<int64_t> sk1(k1), sk2(k2);

    for (int i = 1; i <= n1; ++i) {
      sk1.update(i, i);
    }

    for (int i = 1; i < n2; ++i) { // we'll add a very heavy one later
      sk2.update(-i, i + 1000.0);
    }
    sk2.update(-n2, 1000000.0);

    var_opt_union<int64_t> u(k1);
    u.update(sk1);
    u.update(sk2);

    var_opt_sketch<int64_t> result = u.get_result();
    CPPUNIT_ASSERT_EQUAL(n1 + n2, result.get_n());
    CPPUNIT_ASSERT_EQUAL(k2, result.get_k()); // heavy enough the result pulls back to k2

    u.reset();
    result = u.get_result();
    CPPUNIT_ASSERT_EQUAL((uint64_t) 0, result.get_n());
    CPPUNIT_ASSERT_EQUAL(k1, result.get_k()); // union reset so empty result reflects max_k
  }

  void identical_sampling_sketches() {
    uint32_t k = 20;
    uint64_t n = 50;
    var_opt_sketch<int> sk = create_unweighted_sketch(k, n);

    var_opt_union<int> u(k);
    u.update(sk);
    u.update(sk);

    var_opt_sketch<int> result = u.get_result();
    double expected_wt = 2.0 * n;
    subset_summary ss = result.estimate_subset_sum([](int x){return true;});
    CPPUNIT_ASSERT_EQUAL(2 * n, result.get_n());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(expected_wt, ss.total_sketch_weight, EPS);

    // add another sketch, such that sketch_tau < outer_tau
    sk = create_unweighted_sketch(k, k + 1); // tau = (k + 1) / k
    u.update(sk);
    result = u.get_result();
    expected_wt = (2.0 * n) + k + 1;
    ss = result.estimate_subset_sum([](int x){return true;});
    CPPUNIT_ASSERT_EQUAL((2 * n) + k + 1, result.get_n());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(expected_wt, ss.total_sketch_weight, EPS);
  }

  void small_sampling_sketch() {
    uint32_t k_small = 16;
    uint32_t k_max = 128;
    uint64_t n1 = 32;
    uint64_t n2 = 64;

    var_opt_sketch<float> sk(k_small);
    for (int i = 0; i < n1; ++i) { sk.update(i); }
    sk.update(-1, n1 * n1); // add a heavy item

    var_opt_union<float> u(k_max);
    u.update(sk);

    // another one, but different n to get a different per-item weight
    var_opt_sketch<float> sk2(k_small);
    for (int i = 0; i < n2; ++i) { sk2.update(i); }
    u.update(sk2);

    // should trigger migrate_marked_items_by_decreasing_k()
    var_opt_sketch<float> result = u.get_result();
    CPPUNIT_ASSERT_EQUAL(n1 + n2 + 1, result.get_n());
  
    double expected_wt = 1.0 * (n1 + n2); // n1 + n2 light items, ignore the heavy one
    subset_summary ss = result.estimate_subset_sum([](float x){return x >= 0;});
    CPPUNIT_ASSERT_DOUBLES_EQUAL(expected_wt, ss.estimate, EPS);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(expected_wt + (n1 * n1), ss.total_sketch_weight, EPS);
    CPPUNIT_ASSERT_LESS(k_max, result.get_k());

    // check tha tmark information is preserved as expected
    compare_serialization_deserialization(u, false);
  }

  void serialize_empty() {
    var_opt_union<std::string> u(100);

    compare_serialization_deserialization(u);
  }

  void serialize_exact() {
    uint32_t k = 100;
    var_opt_union<int> u(k);
    var_opt_sketch<int> sk = create_unweighted_sketch(k, k / 2);
    u.update(sk);

    compare_serialization_deserialization(u);
  }

  void serialize_sampling() {
    uint32_t k = 100;
    var_opt_union<int> u(k);
    var_opt_sketch<int> sk = create_unweighted_sketch(k, 2 * k);
    u.update(sk);

    compare_serialization_deserialization(u);
  }

/**********************************************************/


  void test_union() {
    var_opt_union<int> u(10);

    var_opt_sketch<int> sk = create_unweighted_sketch(9, 100);
    u.update(sk);
    std::cout << u.to_string() << std::endl;

    auto vec = u.serialize();
    std::cout << vec.size() << "\t" << vec.capacity() << "\t" << vec.empty() << std::endl;
  }

  void deserialize_exact_from_java() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "varopt_string_exact.bin", std::ios::binary);
    var_opt_sketch<std::string> sketch = var_opt_sketch<std::string>::deserialize(is);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL((uint32_t) 1024, sketch.get_k());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 200, sketch.get_n());
    CPPUNIT_ASSERT_EQUAL((uint32_t) 200, sketch.get_num_samples());
    subset_summary ss = sketch.estimate_subset_sum([](std::string x){ return true; });

    double tgt_wt = 0.0;
    for (int i = 1; i <= 200; ++i) { tgt_wt += 1000.0 / i; }
    CPPUNIT_ASSERT_DOUBLES_EQUAL(tgt_wt, ss.total_sketch_weight, EPS);
  }

  void deserialize_sampling_from_java() {
    std::ifstream is;
    is.exceptions(std::ios::failbit | std::ios::badbit);
    is.open(testBinaryInputPath + "varopt_long_sampling.bin", std::ios::binary);
    var_opt_sketch<int64_t> sketch = var_opt_sketch<int64_t>::deserialize(is);
    CPPUNIT_ASSERT(!sketch.is_empty());
    CPPUNIT_ASSERT_EQUAL((uint32_t) 1024, sketch.get_k());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 2003, sketch.get_n());
    CPPUNIT_ASSERT_EQUAL(sketch.get_k(), sketch.get_num_samples());
    subset_summary ss = sketch.estimate_subset_sum([](int64_t x){ return true; });
    CPPUNIT_ASSERT_DOUBLES_EQUAL(332000.0, ss.estimate, EPS);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(332000.0, ss.total_sketch_weight, EPS);

    ss = sketch.estimate_subset_sum([](int64_t x){ return x < 0; });
    CPPUNIT_ASSERT_DOUBLES_EQUAL(330000.0, ss.estimate, 0.0);

    ss = sketch.estimate_subset_sum([](int64_t x){ return x >= 0; });
    CPPUNIT_ASSERT_DOUBLES_EQUAL(2000.0, ss.estimate, EPS);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(var_opt_union_test);

} /* namespace datasketches */
