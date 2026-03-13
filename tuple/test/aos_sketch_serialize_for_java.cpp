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
#include <fstream>
#include <initializer_list>

#include "array_of_strings_sketch.hpp"

namespace datasketches {

using aos_sketch = update_array_of_strings_tuple_sketch<>;
using array_of_strings = array<std::string>;

static array_of_strings make_array(std::initializer_list<std::string> items) {
  array_of_strings array(static_cast<uint8_t>(items.size()), "");
  size_t i = 0;
  for (const auto& item: items) {
    array[static_cast<uint8_t>(i)] = item;
    ++i;
  }
  return array;
}

TEST_CASE("aos sketch generate one value", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    auto sketch = aos_sketch::builder().build();
    for (unsigned i = 0; i < n; ++i) {
      array_of_strings key(1, "");
      key[0] = std::to_string(i);
      array_of_strings value(1, "");
      value[0] = "value" + std::to_string(i);
      sketch.update(hash_array_of_strings_key(key), value);
    }
    REQUIRE(sketch.is_empty() == (n == 0));
    REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.03));
    std::ofstream os("aos_1_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    compact_array_of_strings_sketch(sketch).serialize(os, default_array_of_strings_serde<>());
  }
}

TEST_CASE("aos sketch generate three values", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    auto sketch = aos_sketch::builder().build();
    for (unsigned i = 0; i < n; ++i) {
      array_of_strings key(1, "");
      key[0] = std::to_string(i);
      array_of_strings value(3, "");
      value[0] = "a" + std::to_string(i);
      value[1] = "b" + std::to_string(i);
      value[2] = "c" + std::to_string(i);
      sketch.update(hash_array_of_strings_key(key), value);
    }
    REQUIRE(sketch.is_empty() == (n == 0));
    REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.03));
    std::ofstream os("aos_3_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    compact_array_of_strings_sketch(sketch).serialize(os, default_array_of_strings_serde<>());
  }
}

TEST_CASE("aos sketch generate non-empty no entries", "[serialize_for_java]") {
  auto sketch = aos_sketch::builder()
    .set_lg_k(12)
    .set_resize_factor(resize_factor::X8)
    .set_p(0.01f)
    .build();
  array_of_strings key(1, "");
  key[0] = "key1";
  array_of_strings value(1, "");
  value[0] = "value1";
  sketch.update(hash_array_of_strings_key(key), value);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_num_retained() == 0);
  std::ofstream os("aos_1_non_empty_no_entries_cpp.sk", std::ios::binary);
  compact_array_of_strings_sketch(sketch).serialize(os, default_array_of_strings_serde<>());
}

TEST_CASE("aos sketch generate multi key strings", "[serialize_for_java]") {
  const unsigned n_arr[] = {0, 1, 10, 100, 1000, 10000, 100000, 1000000};
  for (const unsigned n: n_arr) {
    auto sketch = aos_sketch::builder().build();
    for (unsigned i = 0; i < n; ++i) {
      array_of_strings key(2, "");
      key[0] = "key" + std::to_string(i);
      key[1] = "subkey" + std::to_string(i % 10);
      array_of_strings value(1, "");
      value[0] = "value" + std::to_string(i);
      sketch.update(hash_array_of_strings_key(key), value);
    }
    REQUIRE(sketch.is_empty() == (n == 0));
    REQUIRE(sketch.get_estimate() == Approx(n).margin(n * 0.03));
    std::ofstream os("aos_multikey_n" + std::to_string(n) + "_cpp.sk", std::ios::binary);
    compact_array_of_strings_sketch(sketch).serialize(os, default_array_of_strings_serde<>());
  }
}

TEST_CASE("aos sketch generate unicode strings", "[serialize_for_java]") {
  auto sketch = aos_sketch::builder().build();
  sketch.update(
    hash_array_of_strings_key(make_array({u8"키", u8"열쇠"})),
    make_array({u8"밸류", u8"값"})
  );
  sketch.update(
    hash_array_of_strings_key(make_array({u8"🔑", u8"🗝️"})),
    make_array({u8"📦", u8"🎁"})
  );
  sketch.update(
    hash_array_of_strings_key(make_array({u8"ключ1", u8"ключ2"})),
    make_array({u8"ценить1", u8"ценить2"})
  );
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_num_retained() == 3);
  std::ofstream os("aos_unicode_cpp.sk", std::ios::binary);
  compact_array_of_strings_sketch(sketch).serialize(os, default_array_of_strings_serde<>());
}

TEST_CASE("aos sketch generate empty strings", "[serialize_for_java]") {
  auto sketch = aos_sketch::builder().build();
  sketch.update(hash_array_of_strings_key(make_array({""})), make_array({"empty_key_value"}));
  sketch.update(hash_array_of_strings_key(make_array({"empty_value_key"})), make_array({""}));
  sketch.update(hash_array_of_strings_key(make_array({"", ""})), make_array({"", ""}));
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.get_num_retained() == 3);
  std::ofstream os("aos_empty_strings_cpp.sk", std::ios::binary);
  compact_array_of_strings_sketch(sketch).serialize(os, default_array_of_strings_serde<>());
}

} /* namespace datasketches */
