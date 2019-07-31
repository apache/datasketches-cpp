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
//#include "CouponList.hpp"
//#include "CouponHashSet.hpp"
//#include "HllArray.hpp"

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
    sk.serializeUpdatable(ss1);
    std::pair<byte_ptr_with_deleter, size_t> ser1 = sk.serializeUpdatable();

    std::stringstream ss;
    sk.serializeUpdatable(ss);
    std::string str = ss.str();


    hll_sketch sk2 = hll_sketch::deserialize(ser1.first.get(), ser1.second);
    //hll_sketch sk2 = HllSketch::deserialize(ss1);
    
    //std::stringstream ss2;
    //sk2->serializeUpdatable(ss2);
    std::pair<byte_ptr_with_deleter, size_t> ser2 = sk.serializeUpdatable();

    // std::string b1 = ss1.str();
    // std::string b2 = ss2.str();
    // CPPUNIT_ASSERT_EQUAL(b1.length(), b2.length());
    // int len = b1.length();

    CPPUNIT_ASSERT_EQUAL(ser1.second, ser2.second);
    int len = ser1.second;
    uint8_t* b1 = ser1.first.get();
    uint8_t* b2 = ser2.first.get();

    for (int i = 0; i < len; ++i) {
      if (b1[i] != b2[i]) {
        hll_sketch from_ss = hll_sketch::deserialize(ss1);
        std::cout << "ser3:\n";
        std::pair<byte_ptr_with_deleter, size_t> ser3 = from_ss.serializeUpdatable();
        uint8_t* b3 = ser3.first.get();
        
        std::cerr << "Mismatch at byte " << i << "\n";
        std::cerr << "b1: " << b1[i] << "\n";
        std::cerr << "b2: " << b2[i] << "\n";
        std::cerr << "b3: " << b3[i] << "\n";
        //std::cerr << sk->to_string(true, true, true, true) << "\n";
        //std::cerr << sk2->to_string(true, true, true, true) << "\n";
        //std::cerr << from_ss->to_string(true, true, true, true) << "\n";
        CPPUNIT_ASSERT_EQUAL(b1[i], b2[i]);
      }
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
    CPPUNIT_ASSERT(sk.isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getLowerBound(1), 7.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getEstimate(), 7.0, 1e-6); // java: 7.000000104308129
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getUpperBound(1), 7.000350, 1e-5); // java: 7.000349609067664

    /*
    HllSketchPvt* s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == LIST);
    CouponList* cl = (CouponList*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(cl->getCouponCount(), 7);
    */
    ifs.close();

    ifs.open(inputPath + "compact_set_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getLowerBound(1), 24.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getEstimate(), 24.0, 1e-5); // java: 24.00000137090692
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getUpperBound(1), 24.001200, 1e-5); // java: 24.0011996729902

    /*
    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == SET);
    CouponHashSet* chs = (CouponHashSet*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(chs->getCouponCount(), 24);
    */
    ifs.close();

    ifs.open(inputPath + "updatable_set_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getLowerBound(1), 24.0, 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getEstimate(), 24.0, 1e-5); // java: 24.00000137090692
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getUpperBound(1), 24.001200, 1e-5); // java: 24.0011996729902

    /*
    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == SET);
    chs = (CouponHashSet*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(chs->getCouponCount(), 24);
    */
    ifs.close();


    ifs.open(inputPath + "array6_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    /*
    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_6);
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == HLL);
    HllArray* ha = (HllArray*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 0);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    */
    ifs.close();


    ifs.open(inputPath + "compact_array4_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    /*
    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_4);
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == HLL);
    ha = (HllArray*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 3);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    */
    ifs.close();


    ifs.open(inputPath + "updatable_array4_from_java.bin", std::ios::binary);
    sk = hll_sketch::deserialize(ifs);
    CPPUNIT_ASSERT(sk.isEmpty() == false);
    CPPUNIT_ASSERT_EQUAL(sk.getLgConfigK(), 8);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getLowerBound(1), 9589.968564, 1e-5); // java: 9589.968564432073
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getEstimate(), 10089.150211, 1e-5); // java: 10089.1502113328
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk.getUpperBound(1), 10642.370492, 1e-5); // java: 10642.370491998483

    /*
    s = static_cast<HllSketchPvt*>(sk.get());
    CPPUNIT_ASSERT(sk->getTgtHllType() == HLL_4);
    CPPUNIT_ASSERT(s->hllSketchImpl->getCurMode() == HLL);
    ha = (HllArray*) s->hllSketchImpl;
    CPPUNIT_ASSERT_EQUAL(ha->getCurMin(), 3);
    CPPUNIT_ASSERT_EQUAL(ha->getNumAtCurMin(), 1);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ0(), 4.507751, 1e-6); // java: 4.50775146484375
    CPPUNIT_ASSERT_DOUBLES_EQUAL(ha->getKxQ1(), 0.0, 0.0);
    */
    ifs.close();
  }

  void toFrom(const int lgConfigK, const TgtHllType tgtHllType, const int n) {
    hll_sketch src(lgConfigK, tgtHllType);
    for (int i = 0; i < n; ++i) {
      src.update(i);
    }

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    src.serializeCompact(ss);
    hll_sketch dst = hll_sketch::deserialize(ss);
    checkSketchEquality(src, dst);

    std::pair<byte_ptr_with_deleter, const size_t> bytes1 = src.serializeCompact();
    dst = hll_sketch::deserialize(bytes1.first.get(), bytes1.second);
    checkSketchEquality(src, dst);

    ss.clear();
    src.serializeUpdatable(ss);
    dst = hll_sketch::deserialize(ss);
    checkSketchEquality(src, dst);

    std::pair<byte_ptr_with_deleter, const size_t> bytes2 = src.serializeUpdatable();
    dst = hll_sketch::deserialize(bytes2.first.get(), bytes2.second);
    checkSketchEquality(src, dst);
  }

  void checkSketchEquality(hll_sketch& sk1, hll_sketch& sk2) {
    //HllSketchPvt* sk1 = static_cast<HllSketchPvt*>(s1.get());
    //HllSketchPvt* sk2 = static_cast<HllSketchPvt*>(s2.get());

    CPPUNIT_ASSERT_EQUAL(sk1.getLgConfigK(), sk2.getLgConfigK());
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.getLowerBound(1), sk2.getLowerBound(1), 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.getEstimate(), sk2.getEstimate(), 0.0);
    CPPUNIT_ASSERT_DOUBLES_EQUAL(sk1.getUpperBound(1), sk2.getUpperBound(1), 0.0);
    //CPPUNIT_ASSERT_EQUAL(sk1->getCurrentMode(), sk2->getCurrentMode());
    CPPUNIT_ASSERT_EQUAL(sk1.getTgtHllType(), sk2.getTgtHllType());

    /*
    if (sk1->getCurrentMode() == LIST) {
      CouponList* cl1 = static_cast<CouponList*>(sk1->hllSketchImpl);
      CouponList* cl2 = static_cast<CouponList*>(sk2->hllSketchImpl);
      CPPUNIT_ASSERT_EQUAL(cl1->getCouponCount(), cl2->getCouponCount());
    } else if (sk1->getCurrentMode() == SET) {
      CouponHashSet* chs1 = static_cast<CouponHashSet*>(sk1->hllSketchImpl);
      CouponHashSet* chs2 = static_cast<CouponHashSet*>(sk2->hllSketchImpl);
      CPPUNIT_ASSERT_EQUAL(chs1->getCouponCount(), chs2->getCouponCount());
    } else { // sk1->getCurrentMode() == HLL      
      HllArray* ha1 = static_cast<HllArray*>(sk1->hllSketchImpl);
      HllArray* ha2 = static_cast<HllArray*>(sk2->hllSketchImpl);
      CPPUNIT_ASSERT_EQUAL(ha1->getCurMin(), ha2->getCurMin());
      CPPUNIT_ASSERT_EQUAL(ha1->getNumAtCurMin(), ha2->getNumAtCurMin());
      CPPUNIT_ASSERT_DOUBLES_EQUAL(ha1->getKxQ0(), ha2->getKxQ0(), 0);
      CPPUNIT_ASSERT_DOUBLES_EQUAL(ha1->getKxQ1(), ha2->getKxQ1(), 0);
    }
    */
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
