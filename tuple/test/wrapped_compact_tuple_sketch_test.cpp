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

#include <string>
#include <vector>

#include <catch2/catch.hpp>
#include <test_allocator.hpp>
#include <tuple_a_not_b.hpp>
#include <tuple_intersection.hpp>
#include <tuple_sketch.hpp>
#include <tuple_union.hpp>

namespace datasketches {

template<typename Expected, typename Actual>
void check_same_entries(const Expected& expected, const Actual& actual) {
  REQUIRE(actual.get_num_retained() == expected.get_num_retained());
  auto expected_it = expected.begin();
  auto actual_it = actual.begin();
  while (expected_it != expected.end()) {
    REQUIRE(actual_it != actual.end());
    REQUIRE(actual_it->first == expected_it->first);
    REQUIRE(actual_it->second == expected_it->second);
    ++expected_it;
    ++actual_it;
  }
  REQUIRE(actual_it == actual.end());
}

TEST_CASE("wrapped tuple sketch: empty, exact and estimation", "[tuple_sketch]") {
  {
    auto compact = update_tuple_sketch<float>::builder().build().compact();
    auto bytes = compact.serialize();
    auto wrapped = wrapped_compact_tuple_sketch<float>::wrap(bytes.data(), bytes.size());
    REQUIRE(wrapped.is_empty());
    REQUIRE(wrapped.is_ordered());
    REQUIRE_FALSE(wrapped.is_estimation_mode());
    REQUIRE(wrapped.get_num_retained() == 0);
    REQUIRE(wrapped.get_estimate() == 0);
    REQUIRE(wrapped.begin() == wrapped.end());
  }

  {
    auto update = update_tuple_sketch<float>::builder().build();
    update.update(1, 2.5f);
    auto compact = update.compact();
    auto bytes = compact.serialize();
    auto wrapped = wrapped_compact_tuple_sketch<float>::wrap(bytes.data(), bytes.size());
    REQUIRE_FALSE(wrapped.is_empty());
    REQUIRE(wrapped.is_ordered());
    REQUIRE_FALSE(wrapped.is_estimation_mode());
    REQUIRE(wrapped.get_num_retained() == 1);
    REQUIRE(wrapped.get_theta64() == compact.get_theta64());
    REQUIRE(wrapped.get_seed_hash() == compact.get_seed_hash());
    check_same_entries(compact, wrapped);

    auto it = wrapped.begin();
    auto previous = it++;
    REQUIRE(previous->second == 2.5f);
    REQUIRE(it == wrapped.end());
  }

  {
    auto update = update_tuple_sketch<float>::builder().set_lg_k(6).build();
    for (int i = 0; i < 10000; ++i) update.update(i, static_cast<float>(i));
    auto compact = update.compact();
    auto bytes = compact.serialize();
    auto wrapped = wrapped_compact_tuple_sketch<float>::wrap(bytes.data(), bytes.size());
    REQUIRE_FALSE(wrapped.is_empty());
    REQUIRE(wrapped.is_estimation_mode());
    REQUIRE(wrapped.get_theta64() == compact.get_theta64());
    REQUIRE(wrapped.get_estimate() == compact.get_estimate());
    REQUIRE(wrapped.get_lower_bound(2) == compact.get_lower_bound(2));
    REQUIRE(wrapped.get_upper_bound(2) == compact.get_upper_bound(2));
    check_same_entries(compact, wrapped);
    REQUIRE(wrapped.to_string(true).find("### Retained entries") != std::string::npos);
  }
}

TEST_CASE("wrapped tuple sketch: variable-width summaries", "[tuple_sketch]") {
  auto update = update_tuple_sketch<std::string>::builder().build();
  update.update(1, std::string("a"));
  update.update(2, std::string("variable width"));
  update.update(3, std::string(80, 'x'));
  auto compact = update.compact();
  auto bytes = compact.serialize();
  auto wrapped = wrapped_compact_tuple_sketch<std::string>::wrap(
      bytes.data(), bytes.size());
  check_same_entries(compact, wrapped);
}

TEST_CASE("wrapped tuple sketch: validates the complete buffer", "[tuple_sketch]") {
  auto update = update_tuple_sketch<float>::builder().build();
  update.update(1, 1.0f);
  auto bytes = update.compact().serialize();

  for (size_t size = 0; size < bytes.size(); ++size) {
    REQUIRE_THROWS(wrapped_compact_tuple_sketch<float>::wrap(bytes.data(), size));
  }

  auto with_trailing_byte = bytes;
  with_trailing_byte.push_back(0);
  REQUIRE_THROWS_AS(wrapped_compact_tuple_sketch<float>::wrap(
      with_trailing_byte.data(), with_trailing_byte.size()), std::invalid_argument);

  REQUIRE_THROWS_AS(wrapped_compact_tuple_sketch<float>::wrap(
      bytes.data(), bytes.size(), 123), std::invalid_argument);
  REQUIRE_THROWS(wrapped_compact_tuple_sketch<double>::wrap(bytes.data(), bytes.size()));

  auto invalid_preamble = bytes;
  invalid_preamble[0] = 4;
  REQUIRE_THROWS_AS(wrapped_compact_tuple_sketch<float>::wrap(
      invalid_preamble.data(), invalid_preamble.size()), std::invalid_argument);

  auto string_update = update_tuple_sketch<std::string>::builder().build();
  string_update.update(1, std::string("summary"));
  auto string_bytes = string_update.compact().serialize();
  REQUIRE_THROWS(wrapped_compact_tuple_sketch<std::string>::wrap(
      string_bytes.data(), string_bytes.size() - 1));
}

struct wrapped_intersection_policy {
  void operator()(float& summary, const float& other) const {
    summary += other;
  }
};

TEST_CASE("wrapped tuple sketch: set operations", "[tuple_sketch]") {
  auto a = update_tuple_sketch<float>::builder().build();
  auto b = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 100; ++i) a.update(i, 1.0f);
  for (int i = 50; i < 150; ++i) b.update(i, 1.0f);

  auto a_bytes = a.compact().serialize();
  auto b_bytes = b.compact().serialize();
  auto wrapped_a = wrapped_compact_tuple_sketch<float>::wrap(
      a_bytes.data(), a_bytes.size());
  auto wrapped_b = wrapped_compact_tuple_sketch<float>::wrap(
      b_bytes.data(), b_bytes.size());

  auto tuple_union = datasketches::tuple_union<float>::builder().build();
  tuple_union.update(wrapped_a);
  tuple_union.update(wrapped_b);
  auto union_result = tuple_union.get_result();
  REQUIRE(union_result.get_num_retained() == 150);
  float union_sum = 0;
  for (const auto& entry: union_result) union_sum += entry.second;
  REQUIRE(union_sum == 200.0f);

  tuple_intersection<float, wrapped_intersection_policy> intersection;
  intersection.update(wrapped_a);
  intersection.update(wrapped_b);
  auto intersection_result = intersection.get_result();
  REQUIRE(intersection_result.get_num_retained() == 50);
  for (const auto& entry: intersection_result) REQUIRE(entry.second == 2.0f);

  tuple_a_not_b<float> a_not_b;
  auto difference = a_not_b.compute(wrapped_a, wrapped_b);
  REQUIRE(difference.get_num_retained() == 50);

  auto empty_bytes = update_tuple_sketch<float>::builder().build().compact().serialize();
  auto wrapped_empty = wrapped_compact_tuple_sketch<float>::wrap(
      empty_bytes.data(), empty_bytes.size());
  auto unchanged = a_not_b.compute(wrapped_a, wrapped_empty);
  REQUIRE(unchanged.get_num_retained() == wrapped_a.get_num_retained());
  check_same_entries(wrapped_a, unchanged);
}

TEST_CASE("wrapped tuple sketch: arithmetic iteration does not allocate", "[tuple_sketch]") {
  auto update = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 100; ++i) update.update(i, 1.0f);
  auto bytes = update.compact().serialize();

  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  using Wrapped = wrapped_compact_tuple_sketch<float, test_allocator<float>>;
  auto wrapped = Wrapped::wrap(bytes.data(), bytes.size(), DEFAULT_SEED,
      serde<float>(), test_allocator<float>(0));
  uint32_t count = 0;
  for (const auto& entry: wrapped) {
    REQUIRE(entry.second == 1.0f);
    ++count;
  }
  REQUIRE(count == wrapped.get_num_retained());
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

} /* namespace datasketches */
