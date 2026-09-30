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
#include <cmath>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <iostream>
#include <fstream>
#include <sstream>
#include <utility>

#include "tdigest.hpp"

namespace datasketches {

namespace {
constexpr size_t header_size = 8;
constexpr size_t counts_size = 8;
constexpr size_t min_offset = header_size + counts_size;
constexpr size_t max_offset = min_offset + sizeof(double);
constexpr size_t first_centroid_mean_offset = min_offset + sizeof(double) * 2;
constexpr size_t first_centroid_weight_offset = first_centroid_mean_offset + sizeof(double);
constexpr size_t first_buffered_value_offset = first_centroid_mean_offset;
constexpr size_t single_value_offset = header_size;

template <typename T>
void write_bytes(std::vector<uint8_t>& bytes, size_t offset, T value) {
  std::memcpy(bytes.data() + offset, &value, sizeof(T));
}

template <typename T>
void write_bytes(std::string& data, size_t offset, T value) {
  std::memcpy(&data[offset], &value, sizeof(T));
}
} // namespace

TEST_CASE("empty", "[tdigest]") {
  tdigest_double td(10);
//  std::cout << td.to_string();
  REQUIRE(td.is_empty());
  REQUIRE(td.get_k() == 10);
  REQUIRE(td.get_total_weight() == 0);
  REQUIRE_THROWS_AS(td.get_min_value(), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_max_value(), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_rank(0), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_quantile(0.5), std::runtime_error);
  const double split_points[1] {0};
  REQUIRE_THROWS_AS(td.get_PMF(split_points, 1), std::runtime_error);
  REQUIRE_THROWS_AS(td.get_CDF(split_points, 1), std::runtime_error);
}

TEST_CASE("one value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  REQUIRE(td.get_k() == 100);
  REQUIRE(td.get_total_weight() == 1);
  REQUIRE(td.get_min_value() == 1);
  REQUIRE(td.get_max_value() == 1);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.5);
  REQUIRE(td.get_rank(1.01) == 1);
  REQUIRE(td.get_quantile(0) == 1);
  REQUIRE(td.get_quantile(0.5) == 1);
  REQUIRE(td.get_quantile(1) == 1);
}

TEST_CASE("many values", "[tdigest]") {
  const size_t n = 10000;
  tdigest_double td;
  for (size_t i = 0; i < n; ++i) td.update(i);
  REQUIRE_FALSE(td.is_empty());
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
  REQUIRE(td.get_quantile(0) == 0);
  REQUIRE(td.get_quantile(0.5) == Approx(n / 2).epsilon(0.03));
  REQUIRE(td.get_quantile(0.9) == Approx(n * 0.9).epsilon(0.01));
  REQUIRE(td.get_quantile(0.95) == Approx(n * 0.95).epsilon(0.01));
  REQUIRE(td.get_quantile(1) == n - 1);
  const double split_points[1] {n / 2};
  const auto pmf = td.get_PMF(split_points, 1);
  REQUIRE(pmf.size() == 2);
  REQUIRE(pmf[0] == Approx(0.5).margin(0.0001));
  REQUIRE(pmf[1] == Approx(0.5).margin(0.0001));
  const auto cdf = td.get_CDF(split_points, 1);
  REQUIRE(cdf.size() == 2);
  REQUIRE(cdf[0] == Approx(0.5).margin(0.0001));
  REQUIRE(cdf[1] == 1);
}

TEST_CASE("rank - two values", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(2);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.25);
  REQUIRE(td.get_rank(1.25) == 0.375);
  REQUIRE(td.get_rank(1.5) == 0.5);
  REQUIRE(td.get_rank(1.75) == 0.625);
  REQUIRE(td.get_rank(2) == 0.75);
  REQUIRE(td.get_rank(2.01) == 1);
}

TEST_CASE("rank - repeated value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(1);
  td.update(1);
  td.update(1);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.5);
  REQUIRE(td.get_rank(1.01) == 1);
}

TEST_CASE("rank - repeated block", "[tdigest]") {
  tdigest_double td(100);
  td.update(1);
  td.update(2);
  td.update(2);
  td.update(3);
//  td.compress();
//  std::cout << td.to_string(true);
  REQUIRE(td.get_rank(0.99) == 0);
  REQUIRE(td.get_rank(1) == 0.125);
  REQUIRE(td.get_rank(2) == 0.5);
  REQUIRE(td.get_rank(3) == 0.875);
  REQUIRE(td.get_rank(3.01) == 1);
}

TEST_CASE("merge small", "[tdigest]") {
  tdigest_double td1(10);
  td1.update(1);
  td1.update(2);
  tdigest_double td2(10);
  td2.update(2);
  td2.update(3);
  td1.merge(td2);
  REQUIRE(td1.get_min_value() == 1);
  REQUIRE(td1.get_max_value() == 3);
  REQUIRE(td1.get_total_weight() == 4);
  REQUIRE(td1.get_rank(0.99) == 0);
  REQUIRE(td1.get_rank(1) == 0.125);
  REQUIRE(td1.get_rank(2) == 0.5);
  REQUIRE(td1.get_rank(3) == 0.875);
  REQUIRE(td1.get_rank(3.01) == 1);
}

TEST_CASE("merge preserves deserialized min max with weighted tails", "[tdigest]") {
  tdigest_double source(100);
  source.update(0);
  source.update(50);
  source.update(90);
  auto bytes = source.serialize();
  write_bytes(bytes, min_offset, -1.0);
  write_bytes(bytes, max_offset, 100.0);
  auto other = tdigest_double::deserialize(bytes.data(), bytes.size());
  REQUIRE(other.get_min_value() == -1.0);
  REQUIRE(other.get_max_value() == 100.0);

  tdigest_double empty(100);
  empty.merge(other);
  REQUIRE(empty.get_min_value() == -1.0);
  REQUIRE(empty.get_max_value() == 100.0);

  tdigest_double left(100);
  left.update(10);
  left.update(20);
  left.merge(other);
  REQUIRE(left.get_min_value() == -1.0);
  REQUIRE(left.get_max_value() == 100.0);
}

TEST_CASE("merge large", "[tdigest]") {
  const size_t n = 10000;
  tdigest_double td1;
  tdigest_double td2;
  for (size_t i = 0; i < n / 2; ++i) {
    td1.update(i);
    td2.update(n / 2 + i);
  }
//  std::cout << td1.to_string();
//  std::cout << td2.to_string();
  td1.merge(td2);
//  td1.compress();
//  std::cout << td1.to_string(true);
  REQUIRE(td1.get_total_weight() == n);
  REQUIRE(td1.get_min_value() == 0);
  REQUIRE(td1.get_max_value() == n - 1);
  REQUIRE(td1.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td1.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td1.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td1.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td1.get_rank(n) == 1);
}

TEST_CASE("serialize deserialize stream empty", "[tdigest]") {
  tdigest<double> td(100);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
}

TEST_CASE("serialize deserialize stream single value", "[tdigest]") {
  tdigest<double> td;
  td.update(123);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(deserialized_td.get_k() == 200);
  REQUIRE(deserialized_td.get_total_weight() == 1);
  REQUIRE_FALSE(deserialized_td.is_empty());
  REQUIRE(deserialized_td.get_min_value() == 123);
  REQUIRE(deserialized_td.get_max_value() == 123);
}

TEST_CASE("serialize deserialize stream single value buffered", "[tdigest]") {
  tdigest<double> td;
  td.update(123);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s, true);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(deserialized_td.get_k() == 200);
  REQUIRE(deserialized_td.get_total_weight() == 1);
  REQUIRE_FALSE(deserialized_td.is_empty());
  REQUIRE(deserialized_td.get_min_value() == 123);
  REQUIRE(deserialized_td.get_max_value() == 123);
}

TEST_CASE("serialize deserialize stream many values", "[tdigest]") {
  tdigest<double> td(100);
  for (int i = 0; i < 1000; ++i) td.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
  REQUIRE(td.get_min_value() == deserialized_td.get_min_value());
  REQUIRE(td.get_max_value() == deserialized_td.get_max_value());
  REQUIRE(td.get_rank(500) == deserialized_td.get_rank(500));
  REQUIRE(td.get_quantile(0.5) == deserialized_td.get_quantile(0.5));
}

TEST_CASE("serialize deserialize stream many values with buffer", "[tdigest]") {
  tdigest<double> td(100);
  for (int i = 0; i < 10000; ++i) td.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s, true);
  auto deserialized_td = tdigest<double>::deserialize(s);
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
  REQUIRE(td.get_min_value() == deserialized_td.get_min_value());
  REQUIRE(td.get_max_value() == deserialized_td.get_max_value());
  REQUIRE(td.get_rank(500) == deserialized_td.get_rank(500));
  REQUIRE(td.get_quantile(0.5) == deserialized_td.get_quantile(0.5));
}

TEST_CASE("serialize deserialize bytes empty", "[tdigest]") {
  tdigest<double> td(100);
  auto bytes = td.serialize();
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
}

TEST_CASE("serialize deserialize bytes single value", "[tdigest]") {
  tdigest<double> td(200);
  td.update(123);
  auto bytes = td.serialize();
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized_td.get_k() == 200);
  REQUIRE(deserialized_td.get_total_weight() == 1);
  REQUIRE_FALSE(deserialized_td.is_empty());
  REQUIRE(deserialized_td.get_min_value() == 123);
  REQUIRE(deserialized_td.get_max_value() == 123);
}

TEST_CASE("serialize deserialize bytes single value buffered", "[tdigest]") {
  tdigest<double> td(200);
  td.update(123);
  auto bytes = td.serialize(0, true);
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized_td.get_k() == 200);
  REQUIRE(deserialized_td.get_total_weight() == 1);
  REQUIRE_FALSE(deserialized_td.is_empty());
  REQUIRE(deserialized_td.get_min_value() == 123);
  REQUIRE(deserialized_td.get_max_value() == 123);
}

TEST_CASE("serialize deserialize bytes many values", "[tdigest]") {
  tdigest<double> td(100);
  for (int i = 0; i < 1000; ++i) td.update(i);
  auto bytes = td.serialize();
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
  REQUIRE(td.get_min_value() == deserialized_td.get_min_value());
  REQUIRE(td.get_max_value() == deserialized_td.get_max_value());
  REQUIRE(td.get_rank(500) == deserialized_td.get_rank(500));
  REQUIRE(td.get_quantile(0.5) == deserialized_td.get_quantile(0.5));
}

TEST_CASE("serialize deserialize bytes many values with buffer", "[tdigest]") {
  tdigest<double> td(100);
  for (int i = 0; i < 10000; ++i) td.update(i);
  auto bytes = td.serialize();
  auto deserialized_td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(td.get_k() == deserialized_td.get_k());
  REQUIRE(td.get_total_weight() == deserialized_td.get_total_weight());
  REQUIRE(td.is_empty() == deserialized_td.is_empty());
  REQUIRE(td.get_min_value() == deserialized_td.get_min_value());
  REQUIRE(td.get_max_value() == deserialized_td.get_max_value());
  REQUIRE(td.get_rank(500) == deserialized_td.get_rank(500));
  REQUIRE(td.get_quantile(0.5) == deserialized_td.get_quantile(0.5));
}

TEST_CASE("serialize deserialize steam and bytes equivalence empty", "[tdigest]") {
  tdigest<double> td(100);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto bytes = td.serialize();

  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  auto deserialized_td1 = tdigest<double>::deserialize(s);
  auto deserialized_td2 = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));

  REQUIRE(deserialized_td1.is_empty());
  REQUIRE(deserialized_td2.is_empty());
  REQUIRE(deserialized_td1.get_k() == 100);
  REQUIRE(deserialized_td2.get_k() == 100);
  REQUIRE(deserialized_td1.get_total_weight() == 0);
  REQUIRE(deserialized_td2.get_total_weight() == 0);
}

TEST_CASE("serialize deserialize steam and bytes equivalence", "[tdigest]") {
  tdigest<double> td(100);
  const int n = 1000;
  for (int i = 0; i < n; ++i) td.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s);
  auto bytes = td.serialize();

  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  auto deserialized_td1 = tdigest<double>::deserialize(s);
  auto deserialized_td2 = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));

  REQUIRE_FALSE(deserialized_td1.is_empty());
  REQUIRE(deserialized_td1.get_k() == 100);
  REQUIRE(deserialized_td1.get_total_weight() == n);
  REQUIRE(deserialized_td1.get_min_value() == 0);
  REQUIRE(deserialized_td1.get_max_value() == n - 1);

  REQUIRE_FALSE(deserialized_td2.is_empty());
  REQUIRE(deserialized_td2.get_k() == 100);
  REQUIRE(deserialized_td2.get_total_weight() == n);
  REQUIRE(deserialized_td2.get_min_value() == 0);
  REQUIRE(deserialized_td2.get_max_value() == n - 1);

  REQUIRE(deserialized_td1.get_rank(n / 2) == deserialized_td2.get_rank(n / 2));
  REQUIRE(deserialized_td1.get_quantile(0.5) == deserialized_td2.get_quantile(0.5));
}

TEST_CASE("serialize deserialize steam and bytes equivalence with buffer", "[tdigest]") {
  tdigest<double> td(100);
  const int n = 10000;
  for (int i = 0; i < n; ++i) td.update(i);
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  td.serialize(s, true);
  auto bytes = td.serialize(0, true);

  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  auto deserialized_td1 = tdigest<double>::deserialize(s);
  auto deserialized_td2 = tdigest<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));

  REQUIRE_FALSE(deserialized_td1.is_empty());
  REQUIRE(deserialized_td1.get_k() == 100);
  REQUIRE(deserialized_td1.get_total_weight() == n);
  REQUIRE(deserialized_td1.get_min_value() == 0);
  REQUIRE(deserialized_td1.get_max_value() == n - 1);

  REQUIRE_FALSE(deserialized_td2.is_empty());
  REQUIRE(deserialized_td2.get_k() == 100);
  REQUIRE(deserialized_td2.get_total_weight() == n);
  REQUIRE(deserialized_td2.get_min_value() == 0);
  REQUIRE(deserialized_td2.get_max_value() == n - 1);

  REQUIRE(deserialized_td1.get_rank(n / 2) == deserialized_td2.get_rank(n / 2));
  REQUIRE(deserialized_td1.get_quantile(0.5) == deserialized_td2.get_quantile(0.5));
}

TEST_CASE("deserialize from reference implementation stream double", "[tdigest]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(std::string(TEST_BINARY_INPUT_PATH) + "tdigest_ref_k100_n10000_double.sk", std::ios::binary);
  const auto td = tdigest<double>::deserialize(is);
  const size_t n = 10000;
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

TEST_CASE("deserialize from reference implementation stream float", "[tdigest]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(std::string(TEST_BINARY_INPUT_PATH) + "tdigest_ref_k100_n10000_float.sk", std::ios::binary);
  const auto td = tdigest<float>::deserialize(is);
  const size_t n = 10000;
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

TEST_CASE("deserialize from reference implementation bytes double", "[tdigest]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(std::string(TEST_BINARY_INPUT_PATH) + "tdigest_ref_k100_n10000_double.sk", std::ios::binary);
  std::vector<char> bytes((std::istreambuf_iterator<char>(is)), (std::istreambuf_iterator<char>()));
  const auto td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  const size_t n = 10000;
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

TEST_CASE("deserialize from reference implementation bytes float", "[tdigest]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(std::string(TEST_BINARY_INPUT_PATH) + "tdigest_ref_k100_n10000_float.sk", std::ios::binary);
  std::vector<char> bytes((std::istreambuf_iterator<char>(is)), (std::istreambuf_iterator<char>()));
  const auto td = tdigest<double>::deserialize(bytes.data(), bytes.size());
  const size_t n = 10000;
  REQUIRE(td.get_total_weight() == n);
  REQUIRE(td.get_min_value() == 0);
  REQUIRE(td.get_max_value() == n - 1);
  REQUIRE(td.get_rank(0) == Approx(0).margin(0.0001));
  REQUIRE(td.get_rank(n / 4) == Approx(0.25).margin(0.0001));
  REQUIRE(td.get_rank(n / 2) == Approx(0.5).margin(0.0001));
  REQUIRE(td.get_rank(n * 3 / 4) == Approx(0.75).margin(0.0001));
  REQUIRE(td.get_rank(n) == 1);
}

TEST_CASE("iterate centroids", "[tdigest]") {
  tdigest_double td(100);
  for (int i = 0; i < 10; i++) {
    td.update(i);
  }

  auto centroid_count = 0;
  uint64_t total_weight = 0;
  for (const auto &centroid: td) {
    centroid_count++;
    total_weight += centroid.second;
  }
  // Ensure that centroids are retrieved for a case where there is buffered values
  REQUIRE(centroid_count == 10);
  REQUIRE(td.get_total_weight() == total_weight);
}

TEST_CASE("update rejects positive infinity", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  td.update(2.0);
  td.update(std::numeric_limits<double>::infinity());
  REQUIRE(td.get_total_weight() == 2);
  REQUIRE(td.get_max_value() == 2.0);
}

TEST_CASE("update rejects negative infinity", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  td.update(2.0);
  td.update(-std::numeric_limits<double>::infinity());
  REQUIRE(td.get_total_weight() == 2);
  REQUIRE(td.get_min_value() == 1.0);
}

TEST_CASE("deserialize bytes rejects NaN single value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  auto bytes = td.serialize();
  write_bytes(bytes, single_value_offset, std::numeric_limits<double>::quiet_NaN());
  REQUIRE_THROWS_AS(tdigest_double::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("deserialize stream rejects infinity min", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  td.update(2.0);
  td.update(3.0);
  auto bytes = td.serialize();
  std::string data(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  write_bytes(data, min_offset, std::numeric_limits<double>::infinity());
  std::istringstream is(data, std::ios::binary);
  REQUIRE_THROWS_AS(tdigest_double::deserialize(is), std::invalid_argument);
}

TEST_CASE("deserialize bytes rejects NaN centroid mean", "[tdigest]") {
  tdigest_double td(100);
  for (int i = 0; i < 10; ++i) td.update(i);
  auto bytes = td.serialize();
  write_bytes(bytes, first_centroid_mean_offset, std::numeric_limits<double>::quiet_NaN());
  REQUIRE_THROWS_AS(tdigest_double::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("deserialize bytes rejects NaN buffered value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  td.update(2.0);
  auto bytes = td.serialize(0, true);
  write_bytes(bytes, first_buffered_value_offset, std::numeric_limits<double>::quiet_NaN());
  REQUIRE_THROWS_AS(tdigest_double::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("deserialize bytes rejects infinity single value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  auto bytes = td.serialize();
  write_bytes(bytes, single_value_offset, std::numeric_limits<double>::infinity());
  REQUIRE_THROWS_AS(tdigest_double::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("deserialize bytes rejects NaN max", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  td.update(2.0);
  auto bytes = td.serialize();
  write_bytes(bytes, max_offset, std::numeric_limits<double>::quiet_NaN());
  REQUIRE_THROWS_AS(tdigest_double::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("deserialize bytes rejects infinity max", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  td.update(2.0);
  auto bytes = td.serialize();
  write_bytes(bytes, max_offset, std::numeric_limits<double>::infinity());
  REQUIRE_THROWS_AS(tdigest_double::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("deserialize bytes rejects infinity buffered value", "[tdigest]") {
  tdigest_double td(100);
  td.update(1.0);
  td.update(2.0);
  auto bytes = td.serialize(0, true);
  write_bytes(bytes, first_buffered_value_offset, std::numeric_limits<double>::infinity());
  REQUIRE_THROWS_AS(tdigest_double::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("deserialize bytes rejects zero centroid weight", "[tdigest]") {
  tdigest_double td(100);
  for (int i = 0; i < 10; ++i) td.update(i);
  auto bytes = td.serialize();
  write_bytes(bytes, first_centroid_weight_offset, static_cast<uint64_t>(0));
  REQUIRE_THROWS_AS(tdigest_double::deserialize(bytes.data(), bytes.size()), std::invalid_argument);
}

TEST_CASE("deserialize stream rejects zero centroid weight", "[tdigest]") {
  tdigest_double td(100);
  for (int i = 0; i < 10; ++i) td.update(i);
  auto bytes = td.serialize();
  std::string data(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  write_bytes(data, first_centroid_weight_offset, static_cast<uint64_t>(0));
  std::istringstream is(data, std::ios::binary);
  REQUIRE_THROWS_AS(tdigest_double::deserialize(is), std::invalid_argument);
}

TEST_CASE("quantiles are monotonic and stay within min and max", "[tdigest]") {
  tdigest_double td(100);
  for (int i = 0; i < 10000; ++i) td.update(i);
  double previous = td.get_min_value();
  for (int i = 0; i <= 1000; ++i) {
    const double quantile = td.get_quantile(i / 1000.0);
    REQUIRE(quantile >= previous);
    REQUIRE(quantile >= td.get_min_value());
    REQUIRE(quantile <= td.get_max_value());
    previous = quantile;
  }
}

TEST_CASE("rank below the first centroid stays normalized", "[tdigest]") {
  // The format allows a first centroid heavier than 1. The left tail of get_rank()
  // must be divided by the total weight, the same way the right tail is.
  tdigest_double source(200);
  for (int i = 0; i < 1000; ++i) source.update(i);
  auto bytes = source.serialize();
  double first_mean = 0;
  std::memcpy(&first_mean, bytes.data() + first_centroid_mean_offset, sizeof(double));
  REQUIRE(first_mean == 0);
  write_bytes(bytes, min_offset, -1.0);
  write_bytes(bytes, first_centroid_weight_offset, static_cast<uint64_t>(100));
  const auto td = tdigest_double::deserialize(bytes.data(), bytes.size());
  const double total_weight = static_cast<double>(td.get_total_weight());
  REQUIRE(td.get_rank(-1) == 0.5 / total_weight);
  REQUIRE(td.get_rank(-0.5) == (1.0 + ((100.0 / 2.0 - 1.0) * 0.5)) / total_weight);
  double previous = 0;
  for (int i = 0; i <= 100; ++i) {
    const double rank = td.get_rank(-1.0 + (i / 100.0));
    REQUIRE(rank >= 0);
    REQUIRE(rank <= 1);
    REQUIRE(rank >= previous);
    previous = rank;
  }
}

TEST_CASE("quantile above the last centroid does not exceed max", "[tdigest]") {
  tdigest_double source(200);
  for (int i = 0; i < 1000; ++i) source.update(i);
  auto bytes = source.serialize();
  uint32_t num_centroids = 0;
  std::memcpy(&num_centroids, bytes.data() + header_size, sizeof(uint32_t));
  REQUIRE(num_centroids > 1);
  const size_t last_weight_offset = first_centroid_weight_offset + (num_centroids - 1) * 16;
  write_bytes(bytes, last_weight_offset, static_cast<uint64_t>(100));
  const auto td = tdigest_double::deserialize(bytes.data(), bytes.size());
  double previous = td.get_min_value();
  for (int i = 0; i <= 1000; ++i) {
    const double quantile = td.get_quantile(i / 1000.0);
    REQUIRE(quantile >= previous);
    REQUIRE(quantile <= td.get_max_value());
    previous = quantile;
  }
}

tdigest_double sketch_from_centroids(double min, double max,
    std::initializer_list<std::pair<double, uint64_t>> centroids) {
  const auto num_centroids = static_cast<uint32_t>(centroids.size());
  std::vector<uint8_t> bytes(first_centroid_mean_offset + static_cast<size_t>(num_centroids) * 16, 0);
  bytes[0] = 2; // preamble longs
  bytes[1] = 1; // serial version
  bytes[2] = 20; // sketch type
  write_bytes(bytes, 3, static_cast<uint16_t>(100));
  write_bytes(bytes, header_size, num_centroids);
  write_bytes(bytes, header_size + sizeof(uint32_t), static_cast<uint32_t>(0));
  write_bytes(bytes, min_offset, min);
  write_bytes(bytes, max_offset, max);
  size_t offset = first_centroid_mean_offset;
  for (const auto& centroid : centroids) {
    write_bytes(bytes, offset, centroid.first);
    write_bytes(bytes, offset + sizeof(double), centroid.second);
    offset += 16;
  }
  return tdigest_double::deserialize(bytes.data(), bytes.size());
}

TEST_CASE("quantile with a last centroid of weight 2 returns max", "[tdigest]") {
  // weight == total - 1 and last weight == 2 is 0/0 unless that case returns max.
  const auto td = sketch_from_centroids(0, 20, {{0, 10}, {10, 2}});
  const double quantile = td.get_quantile(11.0 / 12.0);
  REQUIRE_FALSE(std::isnan(quantile));
  REQUIRE(quantile == 20);
}

TEST_CASE("quantile right tail approaches max from below", "[tdigest]") {
  const auto td = sketch_from_centroids(0, 40, {{10, 100}, {20, 100}, {30, 100}});
  REQUIRE(td.get_quantile(0.9) == Approx(34.081632653061224).epsilon(1e-12));
}

TEST_CASE("quantile interpolation weights the nearer centroid more", "[tdigest]") {
  const auto td = sketch_from_centroids(0, 40, {{10, 100}, {20, 100}, {30, 100}});
  // Target weight 80 sits between 10 and 20, closer to 10.
  // weighted_average(10, 70, 20, 30) == 13; the swapped weights would return 17.
  REQUIRE(td.get_quantile(80.0 / 300.0) == 13);
}

} /* namespace datasketches */
