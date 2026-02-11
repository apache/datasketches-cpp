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

#include <catch2/catch.hpp>
#include <sstream>
#include <vector>

// Include all affected sketch types
#include <quantiles_sketch.hpp>
#include <kll_sketch.hpp>
#include <req_sketch.hpp>

namespace datasketches {

/**
 * Test for fix of issue #477:
 * BUG: SIGABRT in deserialize(): dereferencing empty std::optional (libc++ verbose_abort)
 * 
 * These tests exercise the actual deserialization code path that contained the bug.
 * With buggy code (&*tmp on empty optional) and hardening enabled, these will SIGABRT.
 * With fixed code (aligned_storage), these pass normally.
 * 
 * IMPORTANT: These tests actually call deserialize() on multi-item sketches, which
 * exercises the buggy code path where min/max are deserialized.
 */

TEST_CASE("quantiles_sketch: deserialize multi-item sketch", "[deserialize_hardening]") {
  // Create sketch with multiple items (so min/max are stored in serialization)
  quantiles_sketch<double> sketch(128);
  for (int i = 0; i < 1000; i++) {
    sketch.update(static_cast<double>(i));
  }
  
  // Serialize
  auto bytes = sketch.serialize();
  
  // Deserialize - WITH BUGGY CODE AND HARDENING, THIS WILL SIGABRT HERE
  // The bug is: sd.deserialize(is, &*tmp, 1) where tmp is empty optional
  auto sketch2 = quantiles_sketch<double>::deserialize(bytes.data(), bytes.size());
  
  // Verify deserialization worked correctly
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_item() == sketch.get_min_item());
  REQUIRE(sketch2.get_max_item() == sketch.get_max_item());
  REQUIRE(sketch2.get_quantile(0.5) == sketch.get_quantile(0.5));
}

TEST_CASE("quantiles_sketch: deserialize from stream", "[deserialize_hardening]") {
  quantiles_sketch<float> sketch(256);
  for (int i = 0; i < 2000; i++) {
    sketch.update(static_cast<float>(i) * 0.5f);
  }
  
  // Serialize to stream
  std::stringstream ss;
  sketch.serialize(ss);
  
  // Deserialize from stream - exercises the buggy code path
  auto sketch2 = quantiles_sketch<float>::deserialize(ss);
  
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_item() == sketch.get_min_item());
  REQUIRE(sketch2.get_max_item() == sketch.get_max_item());
}

TEST_CASE("kll_sketch: deserialize multi-item sketch", "[deserialize_hardening]") {
  kll_sketch<float> sketch(200);
  for (int i = 0; i < 1500; i++) {
    sketch.update(static_cast<float>(i));
  }
  
  auto bytes = sketch.serialize();
  
  // Deserialize - exercises buggy &*tmp code path
  auto sketch2 = kll_sketch<float>::deserialize(bytes.data(), bytes.size());
  
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_item() == sketch.get_min_item());
  REQUIRE(sketch2.get_max_item() == sketch.get_max_item());
}

TEST_CASE("kll_sketch: deserialize from stream", "[deserialize_hardening]") {
  kll_sketch<int> sketch(400);
  for (int i = 0; i < 3000; i++) {
    sketch.update(i);
  }
  
  std::stringstream ss;
  sketch.serialize(ss);
  
  // Deserialize from stream
  auto sketch2 = kll_sketch<int>::deserialize(ss);
  
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_item() == sketch.get_min_item());
  REQUIRE(sketch2.get_max_item() == sketch.get_max_item());
}

TEST_CASE("req_sketch: deserialize multi-level sketch", "[deserialize_hardening]") {
  // REQ sketch only has the bug when num_levels > 1
  // We need to add enough items to trigger multiple levels
  req_sketch<float> sketch(12);
  for (int i = 0; i < 10000; i++) {
    sketch.update(static_cast<float>(i));
  }
  
  auto bytes = sketch.serialize();
  
  // Deserialize - exercises buggy code path when num_levels > 1
  auto sketch2 = req_sketch<float>::deserialize(bytes.data(), bytes.size());
  
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_item() == sketch.get_min_item());
  REQUIRE(sketch2.get_max_item() == sketch.get_max_item());
}

TEST_CASE("req_sketch: deserialize from stream", "[deserialize_hardening]") {
  req_sketch<double> sketch(20);
  for (int i = 0; i < 15000; i++) {
    sketch.update(static_cast<double>(i) * 0.1);
  }
  
  std::stringstream ss;
  sketch.serialize(ss);
  
  // Deserialize from stream
  auto sketch2 = req_sketch<double>::deserialize(ss);
  
  REQUIRE(sketch2.get_n() == sketch.get_n());
  REQUIRE(sketch2.get_min_item() == sketch.get_min_item());
  REQUIRE(sketch2.get_max_item() == sketch.get_max_item());
}

TEST_CASE("multiple sketch types: stress test", "[deserialize_hardening]") {
  SECTION("quantiles with various sizes") {
    for (int k : {64, 128, 256}) {
      quantiles_sketch<int> sketch(k);
      for (int i = 0; i < 5000; i++) {
        sketch.update(i);
      }
      auto bytes = sketch.serialize();
      auto sketch2 = quantiles_sketch<int>::deserialize(bytes.data(), bytes.size());
      REQUIRE(sketch2.get_n() == sketch.get_n());
    }
  }
  
  SECTION("kll with various sizes") {
    for (int k : {100, 200, 400}) {
      kll_sketch<double> sketch(k);
      for (int i = 0; i < 4000; i++) {
        sketch.update(static_cast<double>(i) / 10.0);
      }
      auto bytes = sketch.serialize();
      auto sketch2 = kll_sketch<double>::deserialize(bytes.data(), bytes.size());
      REQUIRE(sketch2.get_n() == sketch.get_n());
    }
  }
  
  SECTION("req with various sizes") {
    for (int k : {12, 20}) {
      req_sketch<float> sketch(k);
      for (int i = 0; i < 8000; i++) {
        sketch.update(static_cast<float>(i));
      }
      auto bytes = sketch.serialize();
      auto sketch2 = req_sketch<float>::deserialize(bytes.data(), bytes.size());
      REQUIRE(sketch2.get_n() == sketch.get_n());
    }
  }
}

} // namespace datasketches
