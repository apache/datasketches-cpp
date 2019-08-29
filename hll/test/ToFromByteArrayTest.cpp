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

#include "hll.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class ToFromByteArrayTest : public CppUnit::TestFixture {

  // list of values defined at bottom of file
  static const int nArr[]; // = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

  CPPUNIT_TEST_SUITE(ToFromByteArrayTest);
  CPPUNIT_TEST(deserializeFromJava);
  CPPUNIT_TEST(toFromSketch);
  //CPPUNIT_TEST(doubleSerialize);
  CPPUNIT_TEST_SUITE_END();

  void doubleSerialize() {
    hll_sketch sk(9, HLL_8);
    for (int i = 0; i < 1024; ++i) {
      sk.update(i);
    }
    
    std::stringstream ss1;
    sk.serialize_updatable(ss1);
    std::pair<byte_ptr_with_deleter, size_t> ser1 = sk.serialize_updatable();

    std::stringstream ss;
    sk.serialize_updatable(ss);
    std::string str = ss.str();


    hll_sketch sk2 = hll_sketch::deserialize(ser1.first.get(), ser1.second);
    std::pair<byte_ptr_with_deleter, size_t> ser2 = sk.serialize_updatable();

    CPPUNIT_ASSERT_EQUAL(ser1.second, ser2.second);
    int len = ser1.second;
    uint8_t* b1 = ser1.first.get();
    uint8_t* b2 = ser2.first.get();

    for (int i = 0; i < len; ++i) {
      CPPUNIT_ASSERT_EQUAL(b1[i], b2[i]);
    }
  }

  void deserializeFromJava() {
    std::string inputPath;
#ifdef TEST_BINARY_INPUT_PATH
      inputPath = TEST_BINARY_INPUT_PATH;
#else
      inputPath = "test/";
#endif

    std::ifstream ifs;
    ifs.open(inputPath + "list_from_java.bin", std::ios::binary);
    hll_sketch sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.is_empty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.get_lg_config_k(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_lower_bound(1), 7.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_estimate(), 7.0, 1e-6); // java: 7.000000104308129
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_upper_bound(1), 7.000350, 1e-5); // java: 7.000349609067664
    ifs.close();

    ifs.open(inputPath + "compact_set_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.is_empty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.get_lg_config_k(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_lower_bound(1), 24.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_estimate(), 24.0, 1e-5); // java: 24.00000137090692
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_upper_bound(1), 24.001200, 1e-5); // java: 24.0011996729902
    ifs.close();

    ifs.open(inputPath + "updatable_set_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.is_empty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.get_lg_config_k(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_lower_bound(1), 24.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_estimate(), 24.0, 1e-5); // java: 24.00000137090692
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_upper_bound(1), 24.001200, 1e-5); // java: 24.0011996729902
    ifs.close();


    ifs.open(inputPath + "array6_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.is_empty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.get_lg_config_k(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_lower_bound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_estimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_upper_bound(1), 10642.370492, 1e-5); // java: 10642.370491998483
    ifs.close();


    ifs.open(inputPath + "compact_array4_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.is_empty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.get_lg_config_k(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_lower_bound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_estimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_upper_bound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    ifs.close();


    ifs.open(inputPath + "updatable_array4_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.is_empty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.get_lg_config_k(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_lower_bound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_estimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.get_upper_bound(1), 10642.370492, 1e-5); // java: 10642.370491998483
    ifs.close();
  }

  void toFrom(const int lgConfigK, const target_hll_type tgtHllType, const int n) {
    hll_sketch src(lgConfigK, tgtHllType);
    for (int i = 0; i < n; ++i) {
      src.update(i);
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    src.serialize_compact(ss);
    hll_sketch dst = hll_sketch::deserialize(ss);
    checkSketchEquality(src, dst);

    std::pair<byte_ptr_with_deleter, const size_t> bytes1 = src.serialize_compact();
    dst = hll_sketch::deserialize(bytes1.first.get(), bytes1.second);
    checkSketchEquality(src, dst);

    ss.clear();
    src.serialize_updatable(ss);
    dst = hll_sketch::deserialize(ss);
    checkSketchEquality(src, dst);

    std::pair<byte_ptr_with_deleter, const size_t> bytes2 = src.serialize_updatable();
    dst = hll_sketch::deserialize(bytes2.first.get(), bytes2.second);
    checkSketchEquality(src, dst);
  }

  void checkSketchEquality(hll_sketch& sk1, hll_sketch& sk2) {
    CPPUNIT_ASSERT_EQUAL(sk1.get_lg_config_k(), sk2.get_lg_config_k());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.get_lower_bound(1), sk2.get_lower_bound(1), 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.get_estimate(), sk2.get_estimate(), 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.get_upper_bound(1), sk2.get_upper_bound(1), 0.0);
    CPPUNIT_ASSERT_EQUAL(sk1.get_target_type(), sk2.get_target_type());
  }

  void toFromSketch() {
    for (int i = 0; i < 10; ++i) {
      int n = nArr[i];
      for (int lgK = 4; lgK <= 13; ++lgK) {
        toFrom(lgK, HLL_4, n);
        toFrom(lgK, HLL_6, n);
        toFrom(lgK, HLL_8, n);
      }
    }
  }
};

CPPUNIT_TEST_SUITE_REGISTRATION(ToFromByteArrayTest);
  
const int ToFromByteArrayTest::nArr[] = {1, 3, 10, 30, 100, 300, 1000, 3000, 10000, 30000};

} /* namespace datasketches */
