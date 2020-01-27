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

#include <var_opt_sketch.hpp>
#include <var_opt_union.hpp>

#include <string>
#include <sstream>
#include <iostream>

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

class var_opt_sketch_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(var_opt_sketch_test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(vo_union);
  CPPUNIT_TEST_SUITE_END();

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
