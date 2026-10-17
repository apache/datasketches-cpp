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

#include <algorithm>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include <catch2/catch.hpp>

#include "array_of_strings_sketch.hpp"

namespace datasketches {

TEST_CASE("aos update policy", "[tuple_sketch]") {
  default_array_of_strings_update_policy policy;

  SECTION("create empty") {
    auto values = policy.create();
    REQUIRE(values.size() == 0);
  }

  SECTION("replace array") {
    auto values = policy.create();

    array_of_strings input(2, "", std::allocator<std::string>());
    input[0] = "alpha";
    input[1] = "beta";
    policy.update(values, input);
    REQUIRE(values.size() == 2);
    REQUIRE(values[0] == "alpha");
    REQUIRE(values[1] == "beta");
    input[0] = "changed";
    REQUIRE(values[0] == "alpha");

    array_of_strings input2(1, "", std::allocator<std::string>());
    input2[0] = "gamma";
    policy.update(values, input2);
    REQUIRE(values.size() == 1);
    REQUIRE(values[0] == "gamma");
  }

  SECTION("nullptr clears") {
    array_of_strings values(2, "", std::allocator<std::string>());
    values[0] = "one";
    values[1] = "two";

    policy.update(values, nullptr);
    REQUIRE(values.size() == 0);
  }

  SECTION("pointer input copies") {
    auto values = policy.create();

    array_of_strings input(2, "", std::allocator<std::string>());
    input[0] = "first";
    input[1] = "second";
    policy.update(values, &input);
    REQUIRE(values.size() == 2);
    REQUIRE(values[1] == "second");
    input[1] = "changed";
    REQUIRE(values[1] == "second");
  }
}

TEST_CASE("aos sketch update", "[tuple_sketch]") {
  auto make_array = [](std::initializer_list<const char*> entries) {
    array_of_strings array(static_cast<uint8_t>(entries.size()), "", std::allocator<std::string>());
    uint8_t i = 0;
    for (const auto* entry: entries) array[i++] = entry;
    return array;
  };

  SECTION("same key replaces summary") {
    auto sketch = update_array_of_strings_tuple_sketch<>::builder().build();

    sketch.update(
      hash_array_of_strings_key(make_array({"alpha", "beta"})),
      make_array({"first"})
    );
    sketch.update(
      hash_array_of_strings_key(make_array({"alpha", "beta"})),
      make_array({"second", "third"})
    );

    REQUIRE(sketch.get_num_retained() == 1);

    auto it = sketch.begin();
    REQUIRE(it != sketch.end());
    REQUIRE(it->second.size() == 2);
    REQUIRE(it->second[0] == "second");
    REQUIRE(it->second[1] == "third");
  }

  SECTION("distinct keys retain multiple entries") {
    auto sketch = update_array_of_strings_tuple_sketch<>::builder().build();

    sketch.update(
      hash_array_of_strings_key(make_array({"a", "bc"})),
      make_array({"one"})
    );
    sketch.update(
      hash_array_of_strings_key(make_array({"ab", "c"})),
      make_array({"two"})
    );

    REQUIRE(sketch.get_num_retained() == 2);

    bool saw_one = false;
    bool saw_two = false;
    for (const auto& entry: sketch) {
      REQUIRE(entry.second.size() == 1);
      if (entry.second[0] == "one") saw_one = true;
      if (entry.second[0] == "two") saw_two = true;
    }
    REQUIRE(saw_one);
    REQUIRE(saw_two);
  }

  SECTION("empty key") {
    auto sketch = update_array_of_strings_tuple_sketch<>::builder().build();

    sketch.update(hash_array_of_strings_key(make_array({})), make_array({"value"}));
    REQUIRE(sketch.get_num_retained() == 1);

    auto it = sketch.begin();
    REQUIRE(it != sketch.end());
    REQUIRE(it->second.size() == 1);
    REQUIRE(it->second[0] == "value");
  }
}

static array_of_strings to_array(const std::vector<std::string>& items) {
  array_of_strings array(static_cast<uint8_t>(items.size()), "");
  for (size_t i = 0; i < items.size(); ++i) array[static_cast<uint8_t>(i)] = items[i];
  return array;
}

// Java's Util.stringArrHash: XXH64 with seed 0x7A3CCA71 over the UTF-16LE code units of the joined key
static uint64_t java_key_hash(const std::vector<uint16_t>& code_units) {
  std::vector<uint8_t> bytes;
  for (const uint16_t unit: code_units) {
    bytes.push_back(static_cast<uint8_t>(unit));
    bytes.push_back(static_cast<uint8_t>(unit >> 8));
  }
  return XXHash64::hash(bytes.data(), bytes.size(), 0x7A3CCA71);
}

TEST_CASE("aos key hash: UTF-16LE as in Java", "[tuple_sketch]") {
  struct test_case {
    const char* name;
    std::vector<std::string> key;
    std::vector<uint16_t> code_units;
  };
  const std::vector<test_case> test_cases = {
    {"empty array", {}, {}},
    {"empty string", {""}, {}},
    {"ascii with separator", {"a", "b"}, {0x61, 0x2C, 0x62}},
    {"bmp", {"\xED\x82\xA4"}, {0xD0A4}}, // U+D0A4
    {"surrogate pair", {"\xF0\x9F\x94\x91"}, {0xD83D, 0xDD11}}, // U+1F511
    {"mixed with trailing empty element", {"a", "\xF0\x9F\x94\x91", ""}, {0x61, 0x2C, 0xD83D, 0xDD11, 0x2C}},
    {"last code point before surrogates", {"\xED\x9F\xBF"}, {0xD7FF}},
    {"first code point after surrogates", {"\xEE\x80\x80"}, {0xE000}},
    {"replacement character", {"\xEF\xBF\xBD"}, {0xFFFD}},
    {"first supplementary code point", {"\xF0\x90\x80\x80"}, {0xD800, 0xDC00}},
    {"last code point", {"\xF4\x8F\xBF\xBF"}, {0xDBFF, 0xDFFF}},
  };
  for (const auto& test_case: test_cases) {
    INFO(test_case.name);
    REQUIRE(hash_array_of_strings_key(to_array(test_case.key)) == java_key_hash(test_case.code_units));
  }

  SECTION("longer than the internal buffer") {
    // the surrogate pair of U+1F511 straddles the 256-byte buffer boundary
    const std::string str = std::string(127, 'a') + "\xF0\x9F\x94\x91" + std::string(200, 'b');
    std::vector<uint16_t> code_units(127, 0x61);
    code_units.push_back(0xD83D);
    code_units.push_back(0xDD11);
    code_units.insert(code_units.end(), 200, 0x62);
    REQUIRE(hash_array_of_strings_key(to_array({str})) == java_key_hash(code_units));
  }
}

TEST_CASE("aos key hash: invalid UTF-8 replaced as in Java", "[tuple_sketch]") {
  // expected code units are what new String(bytes, StandardCharsets.UTF_8) gives in Java 11-26
  struct test_case {
    const char* name;
    std::string str;
    std::vector<uint16_t> code_units;
  };
  const std::vector<test_case> test_cases = {
    {"invalid lead byte", "\xFF", {0xFFFD}},
    {"lone continuation byte", "\x80", {0xFFFD}},
    {"overlong 2-byte", "\xC0\x80", {0xFFFD, 0xFFFD}},
    {"truncated 3-byte at end", "\xE2\x82", {0xFFFD}},
    {"truncated 3-byte", "\xE2\x82" "A", {0xFFFD, 0x41}},
    {"encoded surrogate", "\xED\xA0\x80", {0xFFFD}},
    {"truncated encoded surrogate", "\xED\xA0" "A", {0xFFFD, 0x41}},
    {"overlong 3-byte", "\xE0\x80\x80", {0xFFFD, 0xFFFD, 0xFFFD}},
    {"truncated 4-byte", "\xF0\x9F\x94" "A", {0xFFFD, 0x41}},
    {"above U+10FFFF", "\xF4\x90\x80\x80", {0xFFFD, 0xFFFD, 0xFFFD, 0xFFFD}},
    {"5-byte form", "\xF8\x88\x80\x80\x80", {0xFFFD, 0xFFFD, 0xFFFD, 0xFFFD, 0xFFFD}},
    {"between valid characters", "a\xE2\x82" "b", {0x61, 0xFFFD, 0x62}},
  };
  for (const auto& test_case: test_cases) {
    INFO(test_case.name);
    REQUIRE(hash_array_of_strings_key(to_array({test_case.str})) == java_key_hash(test_case.code_units));
  }
}

TEST_CASE("aos key hash: elements joined with commas", "[tuple_sketch]") {
  // elements are joined without escaping, as in Java
  REQUIRE(hash_array_of_strings_key(to_array({"a,b"})) == hash_array_of_strings_key(to_array({"a", "b"})));
  REQUIRE(hash_array_of_strings_key(to_array({})) == hash_array_of_strings_key(to_array({""})));
  REQUIRE(hash_array_of_strings_key(to_array({"a", "b"})) != hash_array_of_strings_key(to_array({"b", "a"})));
}

TEST_CASE("aos sketch: serialize deserialize", "[tuple_sketch]") {
  auto make_array = [](std::initializer_list<std::string> entries) {
    array_of_strings array(static_cast<uint8_t>(entries.size()), "", std::allocator<std::string>());
    uint8_t i = 0;
    for (const auto& entry: entries) array[i++] = entry;
    return array;
  };

  auto collect_entries = [](const compact_array_of_strings_tuple_sketch<>& sketch) {
    typedef std::pair<uint64_t, array_of_strings> entry_type;
    std::vector<entry_type> entries;
    for (const auto& entry: sketch) entries.push_back(entry);
    struct entry_less {
      bool operator()(const entry_type& lhs, const entry_type& rhs) const {
        return lhs.first < rhs.first;
      }
    };
    std::sort(entries.begin(), entries.end(), entry_less());
    return entries;
  };

  auto check_round_trip = [&](const compact_array_of_strings_tuple_sketch<>& compact_sketch) {
    std::stringstream ss;
    ss.exceptions(std::ios::failbit | std::ios::badbit);
    compact_sketch.serialize(ss, default_array_of_strings_serde<>());
    auto deserialized_stream = compact_array_of_strings_tuple_sketch<>::deserialize(
      ss, DEFAULT_SEED, default_array_of_strings_serde<>()
    );

    auto bytes = compact_sketch.serialize(0, default_array_of_strings_serde<>());
    auto deserialized_bytes = compact_array_of_strings_tuple_sketch<>::deserialize(
      bytes.data(), bytes.size(), DEFAULT_SEED, default_array_of_strings_serde<>()
    );

    const compact_array_of_strings_tuple_sketch<>* deserialized_list[2] = {
      &deserialized_stream,
      &deserialized_bytes
    };
    for (int list_index = 0; list_index < 2; ++list_index) {
      const compact_array_of_strings_tuple_sketch<>* deserialized = deserialized_list[list_index];
      REQUIRE(compact_sketch.is_empty() == deserialized->is_empty());
      REQUIRE(compact_sketch.is_estimation_mode() == deserialized->is_estimation_mode());
      REQUIRE(compact_sketch.is_ordered() == deserialized->is_ordered());
      REQUIRE(compact_sketch.get_num_retained() == deserialized->get_num_retained());
      REQUIRE(compact_sketch.get_theta() == Approx(deserialized->get_theta()).margin(1e-10));
      REQUIRE(compact_sketch.get_estimate() == Approx(deserialized->get_estimate()).margin(1e-10));
      REQUIRE(compact_sketch.get_lower_bound(1) == Approx(deserialized->get_lower_bound(1)).margin(1e-10));
      REQUIRE(compact_sketch.get_upper_bound(1) == Approx(deserialized->get_upper_bound(1)).margin(1e-10));

      auto original_entries = collect_entries(compact_sketch);
      auto round_trip_entries = collect_entries(*deserialized);
      REQUIRE(original_entries.size() == round_trip_entries.size());
      for (size_t i = 0; i < original_entries.size(); ++i) {
        REQUIRE(original_entries[i].first == round_trip_entries[i].first);
        REQUIRE(original_entries[i].second.size() == round_trip_entries[i].second.size());
        for (size_t j = 0; j < original_entries[i].second.size(); ++j) {
          REQUIRE(original_entries[i].second[static_cast<uint8_t>(j)] ==
            round_trip_entries[i].second[static_cast<uint8_t>(j)]);
        }
      }
    }
  };

  auto run_tests = [&](const update_array_of_strings_tuple_sketch<>& sketch) {
    auto ordered = compact_array_of_strings_sketch(sketch, true);
    auto unordered = compact_array_of_strings_sketch(sketch, false);
    check_round_trip(ordered);
    check_round_trip(unordered);
  };

  SECTION("empty sketch") {
    auto sketch = update_array_of_strings_tuple_sketch<>::builder().build();
    run_tests(sketch);
  }

  SECTION("single entry sketch") {
    auto sketch = update_array_of_strings_tuple_sketch<>::builder().build();
    sketch.update(hash_array_of_strings_key(make_array({"key"})), make_array({"value"}));
    run_tests(sketch);
  }

  SECTION("multiple entries exact mode") {
    auto sketch = update_array_of_strings_tuple_sketch<>::builder().set_lg_k(8).build();
    for (int i = 0; i < 50; ++i) {
      sketch.update(
        hash_array_of_strings_key(make_array({std::string("key-") + std::to_string(i)})),
        make_array({std::string("value-") + std::to_string(i), "extra"})
      );
    }
    REQUIRE_FALSE(sketch.is_estimation_mode());
    run_tests(sketch);
  }

  SECTION("multiple entries estimation mode") {
    auto sketch = update_array_of_strings_tuple_sketch<>::builder().build();
    for (int i = 0; i < 10000; ++i) {
      sketch.update(
        hash_array_of_strings_key(make_array({std::string("key-") + std::to_string(i)})),
        make_array({std::string("value-") + std::to_string(i)})
      );
    }
    REQUIRE(sketch.is_estimation_mode());
    run_tests(sketch);
  }
}

TEST_CASE("aos sketch: serialize with default serde", "[tuple_sketch]") {
  auto sketch = update_array_of_strings_tuple_sketch<>::builder().build();
  sketch.update(hash_array_of_strings_key(to_array({"key"})), to_array({"value1", "value2"}));
  const auto compact_sketch = compact_array_of_strings_sketch(sketch);

  std::stringstream ss;
  ss.exceptions(std::ios::failbit | std::ios::badbit);
  compact_sketch.serialize(ss);
  const auto bytes = compact_sketch.serialize();
  REQUIRE(bytes.size() == ss.str().size());

  const auto from_stream = compact_array_of_strings_tuple_sketch<>::deserialize(ss);
  const auto from_bytes = compact_array_of_strings_tuple_sketch<>::deserialize(bytes.data(), bytes.size());
  for (const auto* deserialized: {&from_stream, &from_bytes}) {
    REQUIRE(deserialized->get_num_retained() == 1);
    const auto& entry = *deserialized->begin();
    REQUIRE(entry.first == compact_sketch.begin()->first);
    REQUIRE(entry.second == to_array({"value1", "value2"}));
  }
}

TEST_CASE("aos serde validation", "[tuple_sketch]") {
  default_array_of_strings_serde<> serde;

  SECTION("too many nodes rejected") {
    array_of_strings array(128, "", std::allocator<std::string>());
    std::stringstream ss;
    ss.exceptions(std::ios::failbit | std::ios::badbit);
    REQUIRE_THROWS_WITH(
      serde.serialize(ss, &array, 1),
      Catch::Matchers::Contains("size exceeds 127")
    );
  }
}

} /* namespace datasketches */
