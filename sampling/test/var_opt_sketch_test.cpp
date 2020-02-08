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

class var_opt_sketch_test: public CppUnit::TestFixture {

  static constexpr double EPS = 1e-13;

  CPPUNIT_TEST_SUITE(var_opt_sketch_test);
  CPPUNIT_TEST(invalid_k);
  CPPUNIT_TEST(bad_ser_ver);
  CPPUNIT_TEST(bad_family);
  CPPUNIT_TEST(bad_prelongs);
  CPPUNIT_TEST(malformed_preamble);
  CPPUNIT_TEST(empty_sketch);
  CPPUNIT_TEST(non_empty_degenerate_sketch);
  CPPUNIT_TEST(invalid_weight);
  CPPUNIT_TEST(corrupt_serialized_weight);
  CPPUNIT_TEST(cumulative_weight);
  CPPUNIT_TEST(under_full_sketch_serialization);
  CPPUNIT_TEST(end_of_warmup_sketch_serialization);
  CPPUNIT_TEST(full_sketch_serialization);
  CPPUNIT_TEST(string_serialization);
  CPPUNIT_TEST(pseudo_light_update);
  CPPUNIT_TEST(pseudo_heavy_update);
  // CPPUNIT_TEST(decrease_k_with_under_full_sketch);
  // CPPUNIT_TEST(decrease_k_with_full_sketch);
  CPPUNIT_TEST(reset);
  CPPUNIT_TEST(estimate_subset_sum);
  // CPPUNIT_TEST(binary_compatibility);
  
  // CPPUNIT_TEST(empty);
  // CPPUNIT_TEST(vo_union);
  CPPUNIT_TEST_SUITE_END();

  var_opt_sketch<int> create_unweighted_sketch(uint32_t k, uint64_t n) {
    var_opt_sketch<int> sk(k);
    for (uint64_t i = 0; i < n; ++i) {
      sk.update(i, 1.0);
    }
    return sk;
  }

  template<typename T, typename S, typename A>
  void check_if_equal(var_opt_sketch<T,S,A>& sk1, var_opt_sketch<T,S,A>& sk2) {
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
      CPPUNIT_ASSERT_EQUAL_MESSAGE("data values differ at sample " + std::to_string(i),
        p1.first, p2.first); 
      CPPUNIT_ASSERT_EQUAL_MESSAGE("weight values differ at sample " + std::to_string(i),
        p1.second, p2.second);
      ++i;
      ++it1;
      ++it2;
    }

    CPPUNIT_ASSERT_MESSAGE("iterators did not end at the same time",
      (it1 == sk1.end()) && (it2 == sk2.end()));
  }

  void invalid_k() {
    std::cerr << "start invalid_k()" << std::endl;
    {
    CPPUNIT_ASSERT_THROW_MESSAGE("constructor failed to catch invalid k = 0",
      var_opt_sketch<int> sk(0),
      std::invalid_argument);

    CPPUNIT_ASSERT_THROW_MESSAGE("constructor failed to catch invalid k < 0 (aka >= 2^31)",
      var_opt_sketch<int> sk(1<<31),
      std::invalid_argument);
    }
    std::cerr << "end invalid_k()" << std::endl;
  }

  void bad_ser_ver() {
    std::cerr << "start bad_ser_ver()" << std::endl;
    {
    var_opt_sketch<int> sk = create_unweighted_sketch(16, 16);
    std::vector<uint8_t> bytes = sk.serialize();
    bytes[1] = 0; // corrupt the serialization version byte

    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch bad serialization version",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    // create a stringstream to check the same
    std::stringstream ss;
    std::string str(bytes.begin(), bytes.end());
    ss.str(str);
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(stream) failed to catch bad serialization version",
      var_opt_sketch<int>::deserialize(ss),
      std::invalid_argument);
    }
    std::cerr << "end bad_ser_ver()" << std::endl;
  }

  void bad_family() {
    std::cerr << "start bad_family()" << std::endl;
    {
    var_opt_sketch<int> sk = create_unweighted_sketch(16, 16);
    std::vector<uint8_t> bytes = sk.serialize();
    bytes[2] = 0; // corrupt the family byte

    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch bad family id",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    std::stringstream ss;
    std::string str(bytes.begin(), bytes.end());
    ss.str(str);
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(stream) failed to catch bad family id",
      var_opt_sketch<int>::deserialize(ss),
      std::invalid_argument);
    }
    std::cerr << "end bad_family()" << std::endl;
  }

  void bad_prelongs() {
    std::cerr << "start bad_prelongs()" << std::endl;
    {
    // The nubmer of preamble longs shares bits with resize_factor, but the latter
    // has no invalid values as it gets 2 bites for 4 enum values.
    var_opt_sketch<int> sk = create_unweighted_sketch(32, 33);
    std::vector<uint8_t> bytes = sk.serialize();

    bytes[0] = 0; // corrupt the preamble longs byte to be too small
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch bad preamble longs",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    bytes[0] = 2; // corrupt the preamble longs byte to 2
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch bad preamble longs",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    bytes[0] = 5; // corrupt the preamble longs byte to be too large
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch bad preamble longs",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);
    }
    std::cerr << "end bad_prelongs()" << std::endl;
  }

  void malformed_preamble() {
    std::cerr << "start malformed_preamble()" << std::endl;
    {
    uint32_t k = 50;
    var_opt_sketch<int> sk = create_unweighted_sketch(k, k);
    const std::vector<uint8_t> src_bytes = sk.serialize();

    // we'll re-use the same bytes several times so we'll use copies
    std::vector<uint8_t> bytes(src_bytes);

    // no items in R, but preamble longs indicates full
    bytes[0] = 4; // PREAMBLE_LONGS_FULL
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch sampling mode with no R items",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    // k = 0
    bytes = src_bytes; 
    *reinterpret_cast<int32_t*>(&bytes[4]) = 0;
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch sampling mode with k = 0",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    // negative H region count in Java (signed ints)
    bytes = src_bytes; 
    *reinterpret_cast<int32_t*>(&bytes[16]) = -1;
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch invalid H count",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    // negative R region count in Java (signed ints)
    bytes = src_bytes; 
    *reinterpret_cast<int32_t*>(&bytes[20]) = -128;
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize(bytes) failed to catch invalid R count",
      var_opt_sketch<int>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);
    }
    std::cerr << "end malformed_preamble()" << std::endl;
  }

  void empty_sketch() {
    std::cerr << "start empty_sketch()" << std::endl;
    {
    var_opt_sketch<std::string> sk(5);
    CPPUNIT_ASSERT_EQUAL((uint64_t) 0, sk.get_n());
    CPPUNIT_ASSERT_EQUAL((uint32_t) 0, sk.get_num_samples());

    std::vector<uint8_t> bytes = sk.serialize();
    CPPUNIT_ASSERT_EQUAL(static_cast<size_t>(1 << 3), bytes.size()); // num bytes in PREAMBLE_LONGS_EMPTY

    var_opt_sketch<std::string> loaded_sk = var_opt_sketch<std::string>::deserialize(bytes.data(), bytes.size());
    CPPUNIT_ASSERT_EQUAL((uint64_t) 0, loaded_sk.get_n());
    CPPUNIT_ASSERT_EQUAL((uint32_t) 0, loaded_sk.get_num_samples());
    }
    std::cerr << "end empty_sketch()" << std::endl;
  }

  void non_empty_degenerate_sketch() {
    std::cerr << "start non_empty_degenerate_sketch()" << std::endl;
    {
    // Make an empty serialized sketch, then extend it to a
    // PREAMBLE_LONGS_WARMUP-sized byte array, with no items.
    // Then clear the empty flag so it will try to load the rest.
    var_opt_sketch<std::string> sk(12, resize_factor::X2);
    std::vector<uint8_t> bytes = sk.serialize();
    while (bytes.size() < 24) { // PREAMBLE_LONGS_WARMUP * 8
      bytes.push_back((uint8_t) 0);
    }
  
    // ensure non-empty -- H and R region sizes already set to 0
    bytes[3] = 0; // set flags bit to not-empty (other bits should already be 0)

    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize() failed to catch non-empty sketch with no items",
      var_opt_sketch<std::string>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);
    }
    std::cerr << "end non_empty_degenerate_sketch()" << std::endl;
  }

  void invalid_weight() {
    std::cerr << "start invalid_weights()" << std::endl;
    {
    var_opt_sketch<std::string> sk(100, resize_factor::X2);
    CPPUNIT_ASSERT_THROW_MESSAGE("update() accepted a negative weight",
      sk.update("invalid_weight", -1.0),
      std::invalid_argument);
    }
    std::cerr << "end invalid_weights()" << std::endl;
  }

  void corrupt_serialized_weight() {
    std::cerr << "start corrupt_serialized_weight()" << std::endl;
    {
    var_opt_sketch<int> sk = create_unweighted_sketch(100, 20);
    auto bytes = sk.to_string();
    
    // weights are in the first double after the preamble
    size_t preamble_bytes = (bytes[0] & 0x3f) << 3;
    *reinterpret_cast<double*>(&bytes[preamble_bytes]) = -1.5;

    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize() failed to catch negative item weight",
      var_opt_sketch<std::string>::deserialize(bytes.data(), bytes.size()),
      std::invalid_argument);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    for (auto& b : bytes) { ss >> b; }
    CPPUNIT_ASSERT_THROW_MESSAGE("deserialize() failed to catch negative item weight",
      var_opt_sketch<std::string>::deserialize(ss),
      std::invalid_argument);
    }
    std::cerr << "end corrupt_serialized_weight()" << std::endl;
  }

  void cumulative_weight() {
    std::cerr << "start cumulative_weight()" << std::endl;
    {
    uint32_t k = 256;
    uint64_t n = 10 * k;
    var_opt_sketch<int> sk(k);

    std::random_device rd; // possibly unsafe in MinGW with GCC < 9.2
    std::mt19937_64 rand(rd());
    std::normal_distribution<double> N(0.0, 1.0);

    double input_sum = 0.0;
    for (size_t i = 0; i < n; ++i) {
      // generate weights aboev and below 1.0 using w ~ exp(5*N(0,1))
      // which covers about 10 orders of magnitude
      double w = std::exp(5 * N(rand));
      input_sum += w;
      sk.update(i, w);
    }

    double output_sum = 0.0;
    for (auto& it : sk) { // std::pair<int, weight>
      output_sum += it.second;
    }
    
    double weight_ratio = output_sum / input_sum;
    CPPUNIT_ASSERT(std::abs(weight_ratio - 1.0) < EPS);
    }
    std::cerr << "end cumulative_weight()" << std::endl;
  }

  void under_full_sketch_serialization() {
    std::cerr << "start under_full_sketch_serialization()" << std::endl;
    {
    var_opt_sketch<int> sk = create_unweighted_sketch(100, 10); // need n < k

    auto bytes = sk.serialize();
    var_opt_sketch<int> sk_from_bytes = var_opt_sketch<int>::deserialize(bytes.data(), bytes.size());
    check_if_equal(sk, sk_from_bytes);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sk.serialize(ss);
    var_opt_sketch<int> sk_from_stream = var_opt_sketch<int>::deserialize(ss);
    check_if_equal(sk, sk_from_stream);
    }
    std::cerr << "end under_full_sketch_serialization()" << std::endl;
  }

  void end_of_warmup_sketch_serialization() {
    std::cerr << "start end_of_warmup_sketch_serialization()" << std::endl;
    {
    var_opt_sketch<int> sk = create_unweighted_sketch(2843, 2843); // need n == k
    auto bytes = sk.serialize();

    // ensure still only 3 preamble longs
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Wrong number of preamble longs at end of warmup mode",
      3, bytes.data()[0] & 0x3f); // PREAMBLE_LONGS_WARMUP

    var_opt_sketch<int> sk_from_bytes = var_opt_sketch<int>::deserialize(bytes.data(), bytes.size());
    check_if_equal(sk, sk_from_bytes);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sk.serialize(ss);
    var_opt_sketch<int> sk_from_stream = var_opt_sketch<int>::deserialize(ss);
    check_if_equal(sk, sk_from_stream);
    }
    std::cerr << "end end_of_warmup_sketch_serialization()" << std::endl;
  }

  void full_sketch_serialization() {
    std::cerr << "start full_sketch_serialization()" << std::endl;
    {
    var_opt_sketch<int> sk = create_unweighted_sketch(32, 32);
    sk.update(100, 100.0);
    sk.update(101, 101.0);

    // first 2 entries should be heavy and in heap order (smallest at root)
    auto it = sk.begin();
    const std::pair<const int, const double> p1 = *it;
    ++it;
    const std::pair<const int, const double> p2 = *it;
    CPPUNIT_ASSERT_DOUBLES_EQUAL(100.0, p1.second, EPS);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(101.0, p2.second, EPS);
    CPPUNIT_ASSERT_EQUAL(100, p1.first);
    CPPUNIT_ASSERT_EQUAL(101, p2.first);

    // check for 4 preamble longs
    auto bytes = sk.serialize();
    CPPUNIT_ASSERT_EQUAL_MESSAGE("Wrong number of preamble longs at end of warmup mode",
      4, bytes.data()[0] & 0x3f); // PREAMBLE_LONGS_WARMUP

    var_opt_sketch<int> sk_from_bytes = var_opt_sketch<int>::deserialize(bytes.data(), bytes.size());
    check_if_equal(sk, sk_from_bytes);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sk.serialize(ss);
    var_opt_sketch<int> sk_from_stream = var_opt_sketch<int>::deserialize(ss);
    check_if_equal(sk, sk_from_stream);
    }
    std::cerr << "end full_sketch_serialization()" << std::endl;
  }

  void string_serialization() {
    std::cerr << "start string_serialization()" << std::endl;
    {
    var_opt_sketch<std::string> sk(5);
    sk.update("a", 1.0);
    sk.update("bc", 1.0);
    sk.update("def", 1.0);
    sk.update("ghij", 1.0);
    sk.update("klmno", 1.0);
    sk.update("heavy item", 100.0);

    auto bytes = sk.serialize();
    var_opt_sketch<std::string> sk_from_bytes = var_opt_sketch<std::string>::deserialize(bytes.data(), bytes.size());
    check_if_equal(sk, sk_from_bytes);

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sk.serialize(ss);
    var_opt_sketch<std::string> sk_from_stream = var_opt_sketch<std::string>::deserialize(ss);
    check_if_equal(sk, sk_from_stream);

    }
    std::cerr << "end string_serialization()" << std::endl;
  }

  void pseudo_light_update() {
    std::cerr << "start pseudo_light_update()" << std::endl;
    {
    uint32_t k = 1024;
    var_opt_sketch<int> sk = create_unweighted_sketch(k, k + 1);
    sk.update(0, 1.0); // k+2nd update

    // check the first weight, assuming all k items are unweighted
    // (and consequently in R).
    // Expected: (k + 2) / |R| = (k + 2) / k
    auto it = sk.begin();
    double wt = (*it).second;
    CPPUNIT_ASSERT_DOUBLES_EQUAL_MESSAGE("weight corruption in pseudo_light_update()",
      ((1.0 * (k + 2)) / k), wt, EPS);
    }
    std::cerr << "end pseudo_light_update()" << std::endl;
  }

  void pseudo_heavy_update() {
    std::cerr << "start pseudo_heavy_update()" << std::endl;
    {
    uint32_t k = 1024;
    double wt_scale = 10.0 * k;
    var_opt_sketch<int> sk = create_unweighted_sketch(k, k + 1);

    // Next k-1 updates should be update_pseudo_heavy_general()
    // Last one should call update_pseudo_heavy_r_eq_1(), since we'll have
    // added k-1 heavy items, leaving only 1 item left in R
    for (int i = 1; i <= k; ++i) {
      sk.update(-i, k + (i * wt_scale));
    }

    auto it = sk.begin();

    // Expected: lightest "heavy" item (first one out): k + 2*wt_scale
    double wt = (*it).second;
    CPPUNIT_ASSERT_DOUBLES_EQUAL_MESSAGE("weight corruption in pseudo_heavy_update()",
      1.0 * (k + (2 * wt_scale)), wt, EPS);

    // we don't know which R item is left, but there should be only one, at the end
    // of the sample set.
    // Expected: k+1 + (min "heavy" item) / |R| = ((k+1) + (k*wt_scale)) / 1 = wt_scale + 2k + 1
    while (it != sk.end()) {
      wt = (*it).second;
      ++it;
    }
    CPPUNIT_ASSERT_DOUBLES_EQUAL_MESSAGE("weight corruption in pseudo_light_update()",
      1.0 + wt_scale + (2 * k), wt, EPS);
    }
    std::cerr << "end pseudo_heavy_update()" << std::endl;
  }

  void reset() {
    std::cerr << "start reset()" << std::endl;
    {
    uint32_t k = 1024;
    uint64_t n1 = 20;
    uint64_t n2 = 2 * k;
    var_opt_sketch<std::string> sk(k);

    // reset from sampling mode
    for (int i = 0; i < n2; ++i) {
      sk.update(std::to_string(i), 100.0 + i);
    }
    CPPUNIT_ASSERT_EQUAL(n2, sk.get_n());
    CPPUNIT_ASSERT_EQUAL(k, sk.get_k());

    sk.reset();
    CPPUNIT_ASSERT_EQUAL((uint64_t) 0, sk.get_n());
    CPPUNIT_ASSERT_EQUAL(k, sk.get_k());

    // reset from exact mode
    for (int i = 0; i < n1; ++i)
      sk.update(std::to_string(i));
    CPPUNIT_ASSERT_EQUAL(n1, sk.get_n());
    CPPUNIT_ASSERT_EQUAL(k, sk.get_k());

    sk.reset();
    CPPUNIT_ASSERT_EQUAL((uint64_t) 0, sk.get_n());
    CPPUNIT_ASSERT_EQUAL(k, sk.get_k());
    }
    std::cerr << "end reset()" << std::endl;
  }

  void estimate_subset_sum() {
    std::cerr << "start estimate_subset_sum()" << std::endl;
    {
    uint32_t k = 10;
    var_opt_sketch<int> sk(k);

    // empty sketch -- all zeros
    subset_summary summary = sk.estimate_subset_sum([](int){ return true; });
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, summary.estimate, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, summary.total_sketch_weight, 0.0);

    // add items, keeping in exact mode
    double total_weight = 0.0;
    for (int i = 1; i <= (k - 1); ++i) {
      sk.update(i, 1.0 * i);
      total_weight += 1.0 * i;
    }

    summary = sk.estimate_subset_sum([](int){ return true; });
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.estimate, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.lower_bound, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.upper_bound, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.total_sketch_weight, 0.0);

    // add a few more items, pushing to sampling mode
    for (int i = k; i <= (k + 1); ++i) {
      sk.update(i, 1.0 * i);
      total_weight += 1.0 * i;
    }

    // predicate always true so estimate == upper bound
    summary = sk.estimate_subset_sum([](int){ return true; });
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.estimate, EPS);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.upper_bound, EPS);
    CPPUNIT_ASSERT_LESS(total_weight, summary.lower_bound); // lower_bound < total_weight
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.total_sketch_weight, EPS);

    // predicate always false so estimate == lower bound == 0.0
    summary = sk.estimate_subset_sum([](int){ return false; });
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, summary.estimate, EPS);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(0.0, summary.lower_bound, EPS);
    CPPUNIT_ASSERT_GREATER(0.0, summary.upper_bound); // upper_bound > 0.0
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.total_sketch_weight, EPS);

    // finally, a non-degenerate predicate
    // insert negative items with identical weights, filter for negative weights only
    for (int i = 1; i <= (k + 1); ++i) {
      sk.update(-i, 1.0 * i);
      total_weight += 1.0 * i;
    }

    summary= sk.estimate_subset_sum([](int x) { return x < 0; });
    CPPUNIT_ASSERT_GREATEREQUAL(summary.lower_bound, summary.estimate); // estimate >= lower_bound)
    CPPUNIT_ASSERT_LESSEQUAL(summary.upper_bound, summary.estimate); // estimate <= upper_bound)

    // allow pretty generous bounds when testing
    CPPUNIT_ASSERT(summary.lower_bound < (total_weight / 1.4));
    CPPUNIT_ASSERT(summary.upper_bound > (total_weight / 2.6));
    CPPUNIT_ASSERT_DOUBLES_EQUAL(total_weight, summary.total_sketch_weight, EPS);

    // and another data type, keeping it in exact mode for simplicity
    var_opt_sketch<bool> sk2(k);
    total_weight = 0.0;
    for (int i = 1; i <= (k - 1); ++i) {
      sk2.update((i % 2) == 0, 1.0 * i);
      total_weight += i;
    }

    summary = sk2.estimate_subset_sum([](bool b){ return !b; });
    CPPUNIT_ASSERT_DOUBLES_EQUAL(summary.lower_bound, summary.estimate, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(summary.upper_bound, summary.estimate, 0.0);
    CPPUNIT_ASSERT_LESS(total_weight, summary.estimate); // exact mode, so know it must be strictly less
    }
    std::cerr << "end estimate_subset_sum()" << std::endl;
  }

  /**********************************************************************/

  void vo_union() {
    int k = 10;
    var_opt_sketch<int> sk(k), sk2(k+3);

    for (int i = 0; i < 10*k; ++i) {
      sk.update(i);
      sk2.update(i);
    }
    sk.update(-1, 10000.0);
    sk2.update(-2, 4000.0);
    std::cerr << sk.to_string() << std::endl;

    var_opt_union<int> vou(k+3);
    std::cerr << vou.to_string() << std::endl;
    vou.update(sk);
    vou.update(sk2);
    std::cerr << vou.to_string() << std::endl;

    var_opt_sketch<int> r = vou.get_result();
    std::cerr << "-----------------------" << std::endl << r.to_string() << std::endl;
  }

  void empty() {
    int k = 10;

    {
    var_opt_sketch<int> sketch(k);

    for (int i = 0; i < 2*k; ++i)
      sketch.update(i);
    sketch.update(1000, 100000.0);

    std::cout << sketch.to_string();

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(ss);
    std::cout << "sketch.serialize() done\n";
    var_opt_sketch<int> sk2 = var_opt_sketch<int>::deserialize(ss);
    std::cout << sk2.to_string() << std::endl;;
    }

    {
    var_opt_sketch<std::string> sk(k);
    std::cout << "Expected size: " << sk.get_serialized_size_bytes() << std::endl;
    std::string x[26];
    x[0]  = std::string("a");
    x[1]  = std::string("b");
    x[2]  = std::string("c");
    x[3]  = std::string("d");
    x[4]  = std::string("e");
    x[5]  = std::string("f");
    x[6]  = std::string("g");
    x[7]  = std::string("h");
    x[8]  = std::string("i");
    x[9]  = std::string("j");
    x[10] = std::string("k");
    x[11] = std::string("l");
    x[12] = std::string("m");
    x[13] = std::string("n");
    x[14] = std::string("o");
    x[15] = std::string("p");
    x[16] = std::string("q");
    x[17] = std::string("r");
    x[18] = std::string("s");
    x[19] = std::string("t");
    x[20] = std::string("u");
    x[21] = std::string("v");
    x[22] = std::string("w");
    x[23] = std::string("x");
    x[24] = std::string("y");
    x[25] = std::string("z");

    for (int i=0; i <11; ++i)
      sk.update(x[i]);
    sk.update(x[11], 10000);
    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    sk.serialize(ss);
    std::cout << "ss size: " << ss.str().length() << std::endl;
    auto vec = sk.serialize();
    std::cout << "Vector size: " << vec.size() << std::endl;
    
    var_opt_sketch<std::string> sk2 = var_opt_sketch<std::string>::deserialize(ss);
    std::cout << sk2.to_string() << std::endl;
    const std::string str("much longer string with luck won't fit nicely in existing structure location");
    sk2.update(str, 1000000);
    }
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(var_opt_sketch_test);

} /* namespace datasketches */
