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
#include <memory>
#include <map>
#include <vector>
#include <set>

#include "tdigest.hpp"

namespace datasketches {

TEST_CASE("tdigest iterator: basic iteration", "[tdigest]") {
  tdigest_double td(100);
  
  // Insert 10 distinct values
  for (int i = 0; i < 10; i++) {
    td.update(static_cast<double>(i));
  }
  
  // Collect all centroids via iteration
  std::map<double, uint64_t> centroids;
  for (const auto&& centroid : td) {
    centroids[centroid.first] = centroid.second;
  }
  
  // Should have collected all 10 distinct values
  REQUIRE(centroids.size() == 10);
  
  // Verify each value was captured correctly
  for (int i = 0; i < 10; i++) {
    REQUIRE(centroids.count(static_cast<double>(i)) == 1);
    REQUIRE(centroids[static_cast<double>(i)] == 1);
  }
}

TEST_CASE("tdigest iterator: explicit begin/end with unique_ptr", "[tdigest]") {
  // This test reproduces the bug scenario found in ClickHouse
  std::unique_ptr<tdigest_double> td(new tdigest_double(100));
  
  // Insert distinct values
  for (int i = 0; i < 10; i++) {
    td->update(static_cast<double>(i));
  }
  
  // Use explicit begin/end iterators
  auto it = td->begin();
  auto end_it = td->end();
  
  std::vector<double> means;
  std::vector<uint64_t> weights;
  
  while (it != end_it) {
    // Before the fix, accessing it->first would return garbage or same value repeatedly
    double mean = it->first;
    uint64_t weight = it->second;
    means.push_back(mean);
    weights.push_back(weight);
    ++it;
  }
  
  // Should have collected 10 centroids
  REQUIRE(means.size() == 10);
  REQUIRE(weights.size() == 10);
  
  // All means should be distinct (not all zeros or garbage)
  std::set<double> unique_means(means.begin(), means.end());
  REQUIRE(unique_means.size() == 10);
  
  // Verify all expected values are present
  for (int i = 0; i < 10; i++) {
    REQUIRE(unique_means.count(static_cast<double>(i)) == 1);
  }
}

TEST_CASE("tdigest iterator: structured bindings", "[tdigest]") {
  tdigest_double td(100);
  
  for (int i = 0; i < 5; i++) {
    td.update(static_cast<double>(i * 10));
  }
  
  std::vector<std::pair<double, uint64_t>> collected;
  
  // Test structured bindings
  for (auto it = td.begin(); it != td.end(); ++it) {
    const auto& centroid = *it;
    collected.emplace_back(centroid.first, centroid.second);
  }
  
  REQUIRE(collected.size() == 5);
  
  // Verify distinct values were collected
  std::set<double> means;
  for (const auto& pair : collected) {
    means.insert(pair.first);
    REQUIRE(pair.second == 1);  // Each value inserted once
  }
  
  REQUIRE(means.size() == 5);
  for (int i = 0; i < 5; i++) {
    REQUIRE(means.count(static_cast<double>(i * 10)) == 1);
  }
}

TEST_CASE("tdigest iterator: operator-> access", "[tdigest]") {
  tdigest_double td(100);
  
  // Insert values
  for (int i = 1; i <= 10; i++) {
    td.update(static_cast<double>(i * i));  // 1, 4, 9, 16, 25, 36, 49, 64, 81, 100
  }
  
  // Access via operator->
  std::map<double, uint64_t> centroids;
  auto end_it = td.end();
  for (auto it = td.begin(); it != end_it; ++it) {
    // operator-> should return valid values
    centroids[it->first] = it->second;
  }
  
  REQUIRE(centroids.size() == 10);
  
  // Verify the squared values
  for (int i = 1; i <= 10; i++) {
    double expected = static_cast<double>(i * i);
    REQUIRE(centroids.count(expected) == 1);
  }
}

TEST_CASE("tdigest iterator: range-based for with const auto&&", "[tdigest]") {
  tdigest_double td(100);
  
  // Insert values
  for (double d = 0.0; d < 10.0; d += 1.0) {
    td.update(d);
  }
  
  size_t count = 0;
  std::set<double> seen_means;
  
  // This pattern was working in simple tests but failing in optimized builds
  for (const auto&& centroid : td) {
    seen_means.insert(centroid.first);
    count++;
  }
  
  REQUIRE(count == 10);
  REQUIRE(seen_means.size() == 10);
  
  // Verify all values from 0 to 9 are present
  for (int i = 0; i < 10; i++) {
    REQUIRE(seen_means.count(static_cast<double>(i)) == 1);
  }
}

TEST_CASE("tdigest iterator: copy vs reference semantics", "[tdigest]") {
  tdigest_double td(100);
  
  td.update(1.0);
  td.update(2.0);
  td.update(3.0);
  
  auto it = td.begin();
  
  // Store the pair
  auto pair1 = *it;
  double mean1 = pair1.first;
  
  ++it;
  
  // Store another pair
  auto pair2 = *it;
  double mean2 = pair2.first;
  
  ++it;
  
  auto pair3 = *it;
  double mean3 = pair3.first;
  
  // All three means should be distinct
  REQUIRE(mean1 != mean2);
  REQUIRE(mean2 != mean3);
  REQUIRE(mean1 != mean3);
  
  // And they should match our input values
  std::set<double> means = {mean1, mean2, mean3};
  REQUIRE(means.count(1.0) == 1);
  REQUIRE(means.count(2.0) == 1);
  REQUIRE(means.count(3.0) == 1);
}

TEST_CASE("tdigest iterator: empty sketch", "[tdigest]") {
  tdigest_double td(100);
  
  // Empty sketch should have begin() == end()
  REQUIRE(td.begin() == td.end());
  
  // Range-based for should not execute
  size_t count = 0;
  for (const auto&& centroid : td) {
    (void)centroid;  // Silence unused warning
    count++;
  }
  REQUIRE(count == 0);
}

TEST_CASE("tdigest iterator: single value", "[tdigest]") {
  tdigest_double td(100);
  td.update(42.0);
  
  size_t count = 0;
  double captured_mean = 0.0;
  uint64_t captured_weight = 0;
  
  for (const auto&& centroid : td) {
    captured_mean = centroid.first;
    captured_weight = centroid.second;
    count++;
  }
  
  REQUIRE(count == 1);
  REQUIRE(captured_mean == 42.0);
  REQUIRE(captured_weight == 1);
}

TEST_CASE("tdigest iterator: large dataset", "[tdigest]") {
  tdigest_double td(100);
  
  // Insert 1000 distinct values
  for (int i = 0; i < 1000; i++) {
    td.update(static_cast<double>(i));
  }
  
  // Iterator should provide compressed centroids (not all 1000)
  size_t centroid_count = 0;
  std::set<double> unique_means;
  uint64_t total_weight = 0;
  
  for (const auto&& centroid : td) {
    unique_means.insert(centroid.first);
    total_weight += centroid.second;
    centroid_count++;
  }
  
  // Should have fewer centroids than input values due to compression
  REQUIRE(centroid_count < 1000);
  REQUIRE(centroid_count > 0);
  
  // Total weight should equal number of input values
  REQUIRE(total_weight == 1000);
  
  // All means should be unique (no duplicates)
  REQUIRE(unique_means.size() == centroid_count);
}

} // namespace datasketches
