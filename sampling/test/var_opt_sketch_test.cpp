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

  static constexpr double EPS = 1e-10;

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
  // CPPUNIT_TEST(under_full_sketch_serialization);
  // CPPUNIT_TEST(end_of_warmup_sketch_serialization);
  // CPPUNIT_TEST(full_sketch_serialization);
  // CPPUNIT_TEST(pseudo_light_update);
  // CPPUNIT_TEST(pseudo_heavy_update);
  // CPPUNIT_TEST(decrease_k_with_under_full_sketch);
  // CPPUNIT_TEST(decrease_k_with_full_sketch);
  // CPPUNIT_TEST(reset);
  // CPPUNIT_TEST(estimate_subset_sum);
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
    for (auto& it : sk) { // pair<int, weight>
      output_sum += it.second;
    }
    
    double weight_ratio = output_sum / input_sum;
    CPPUNIT_ASSERT(std::abs(weight_ratio - 1.0) < EPS);
    }
    std::cerr << "end cumulative_weight()" << std::endl;
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
