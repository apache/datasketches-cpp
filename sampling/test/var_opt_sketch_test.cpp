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

#include "counting_allocator.hpp"

#include <var_opt_sketch.hpp>

#include <string>
#include <sstream>
#include <iostream>

#ifdef TEST_BINARY_INPUT_PATH
static std::string testBinaryInputPath = TEST_BINARY_INPUT_PATH;
#else
static std::string testBinaryInputPath = "test/";
#endif

namespace datasketches {

long long int total_allocated_memory;
long long int total_objects_constructed;

class var_opt_sketch_test: public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(var_opt_sketch_test);
  CPPUNIT_TEST(empty);
  CPPUNIT_TEST(mem_test);
  CPPUNIT_TEST_SUITE_END();

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
    std::cerr << "num allocs: " << num_allocs << "\n";
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

  void print_mem_stats() {
    std::cout << "construct(): " << total_objects_constructed << std::endl;
    std::cout << "memory used: " << total_allocated_memory << std::endl;
  }

  void mem_test() {
    int k = 10;

    total_allocated_memory = 0;
    total_objects_constructed = 0;

    typedef var_opt_sketch<std::string, serde<std::string>, counting_allocator<std::string>> var_opt_sketch_a;
    var_opt_sketch_a* s = new (counting_allocator<var_opt_sketch_a>().allocate(1)) var_opt_sketch_a(k);
  
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

    for (int i=0; i <5; ++i)
      s->update(x[i]);
    print_mem_stats();

    for (int i=5; i <11; ++i)
      s->update(x[i]);
    print_mem_stats();

    s->update(x[11], 10000);
    print_mem_stats();

    for (int i=12; i <26; ++i)
      s->update(x[i]);
    print_mem_stats();

    std::stringstream ss(std::ios::in | std::ios::out | std::ios::binary);
    s->serialize(ss);
    std::cout << s->to_string() << std::endl;
    print_mem_stats();

    counting_allocator<var_opt_sketch_a>().destroy(s);
    counting_allocator<var_opt_sketch_a>().deallocate(s, 1);
    print_mem_stats();

    {
      auto sk = var_opt_sketch_a::deserialize(ss);
      print_mem_stats();

      sk.update("qrs");
      print_mem_stats();

      sk.update("zyx");
      print_mem_stats();

      std::cout << sk.to_string() << std::endl;

      auto vec = sk.serialize();
      var_opt_sketch_a sk2 = var_opt_sketch_a::deserialize(vec.data(), vec.size());
      std::cout << sk2.to_string() << std::endl;
    }
    print_mem_stats();
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(var_opt_sketch_test);

} /* namespace datasketches */
