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

using types = array_of_strings_types<std::allocator<char>>;
using array_of_strings = types::array_of_strings;
using string_allocator = types::string_allocator;
using string_type = types::string_type;
using array_allocator = types::array_allocator;

TEST_CASE("aos update policy", "[tuple_sketch]") {
  default_array_of_strings_update_policy<> policy;

  SECTION("create empty") {
    auto values = policy.create();
    REQUIRE(values.size() == 0);
  }

  SECTION("replace array") {
    auto values = policy.create();

    const string_type empty{string_allocator()};
    array_of_strings input(2, empty, array_allocator());
    input[0] = "alpha";
    input[1] = "beta";
    policy.update(values, input);
    REQUIRE(values.size() == 2);
    REQUIRE(values[0] == "alpha");
    REQUIRE(values[1] == "beta");
    input[0] = "changed";
    REQUIRE(values[0] == "alpha");

    array_of_strings input2(1, empty, array_allocator());
    input2[0] = "gamma";
    policy.update(values, input2);
    REQUIRE(values.size() == 1);
    REQUIRE(values[0] == "gamma");
  }

  SECTION("nullptr clears") {
    const string_type empty{string_allocator()};
    array_of_strings values(2, empty, array_allocator());
    values[0] = "one";
    values[1] = "two";

    policy.update(values, nullptr);
    REQUIRE(values.size() == 0);
  }

  SECTION("pointer input copies") {
    auto values = policy.create();

    const string_type empty{string_allocator()};
    array_of_strings input(2, empty, array_allocator());
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
    const string_type empty{string_allocator()};
    array_of_strings array(static_cast<uint8_t>(entries.size()), empty, array_allocator());
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

TEST_CASE("aos sketch: serialize deserialize", "[tuple_sketch]") {
  auto make_array = [](std::initializer_list<std::string> entries) {
    const string_type empty{string_allocator()};
    array_of_strings array(static_cast<uint8_t>(entries.size()), empty, array_allocator());
    uint8_t i = 0;
    for (const auto& entry: entries) {
      array[i++] = string_type(entry.data(), entry.size(), string_allocator());
    }
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

TEST_CASE("aos serde validation", "[tuple_sketch]") {
  default_array_of_strings_serde<> serde;

  SECTION("invalid utf8 rejected") {
    const string_type empty{string_allocator()};
    array_of_strings array(1, empty, array_allocator());
    const string_type invalid_utf8("\xC3\x28", 2, string_allocator());
    array[0] = invalid_utf8;
    std::stringstream ss;
    ss.exceptions(std::ios::failbit | std::ios::badbit);
    REQUIRE_THROWS_WITH(
      serde.serialize(ss, &array, 1),
      Catch::Matchers::Contains("invalid UTF-8")
    );
  }

  SECTION("too many nodes rejected") {
    const string_type empty{string_allocator()};
    array_of_strings array(128, empty, array_allocator());
    std::stringstream ss;
    ss.exceptions(std::ios::failbit | std::ios::badbit);
    REQUIRE_THROWS_WITH(
      serde.serialize(ss, &array, 1),
      Catch::Matchers::Contains("size exceeds 127")
    );
  }
}

} /* namespace datasketches */
