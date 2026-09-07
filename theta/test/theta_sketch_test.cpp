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

#include <istream>
#include <fstream>
#include <sstream>
#include <vector>
#include <stdexcept>
#include <algorithm>

#include <catch2/catch.hpp>
#include <theta_sketch.hpp>

namespace datasketches {

#ifdef TEST_BINARY_INPUT_PATH
const std::string inputPath = TEST_BINARY_INPUT_PATH;
#else
const std::string inputPath = "test/";
#endif

TEST_CASE("theta sketch: empty", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  REQUIRE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 0.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0.0);
  REQUIRE(update_sketch.is_ordered());

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() == 1.0);
  REQUIRE(compact_sketch.get_estimate() == 0.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 0.0);
  REQUIRE(compact_sketch.is_ordered());

  // empty is forced to be ordered
  REQUIRE(update_sketch.compact(false).is_ordered());
}

TEST_CASE("theta sketch: min lg_k", "[theta_sketch]") {
  // lg_k = 4 (nominal 16) is the smallest allowed nominal size, matching Java's
  // ThetaUtil.MIN_LG_NOM_LONGS. Anything below it must throw.
  REQUIRE(theta_constants::MIN_LG_K == 4);
  REQUIRE_THROWS_AS(update_theta_sketch::builder().set_lg_k(theta_constants::MIN_LG_K - 1),
      std::invalid_argument);
  update_theta_sketch min_sketch = update_theta_sketch::builder().set_lg_k(theta_constants::MIN_LG_K).build();
  REQUIRE(min_sketch.get_lg_k() == theta_constants::MIN_LG_K);

  // update well past the nominal size to force estimation mode and exercise the rebuild path,
  // tracking the peak number of retained entries seen between rebuilds.
  const int n = 10000;
  uint32_t max_retained = 0;
  for (int i = 0; i < n; ++i) {
    min_sketch.update(i);
    max_retained = std::max(max_retained, min_sketch.get_num_retained());
  }
  REQUIRE(min_sketch.is_estimation_mode());
  REQUIRE(min_sketch.get_theta() < 1.0);

  // The internal hash table is floored at MIN_LG_ARR (5, i.e. 32 slots), one lg above the
  // nominal size. This is exactly what MIN_LG_ARR guarantees: between rebuilds the sketch holds
  // more than the nominal 2^MIN_LG_K (16) entries, but never more than the 2^MIN_LG_ARR (32)
  // slots of the table. Were the table sized to the nominal 16, it could not retain more than 16.
  REQUIRE(max_retained > (1 << theta_constants::MIN_LG_K));
  REQUIRE(max_retained <= (1 << theta_constants::MIN_LG_ARR));

  // the true count is bracketed by the 2-standard-deviation confidence bounds
  REQUIRE(min_sketch.get_lower_bound(2) <= n);
  REQUIRE(min_sketch.get_upper_bound(2) >= n);

  // trimming reduces the sketch to exactly the nominal number of entries (2^4 = 16)
  min_sketch.trim();
  REQUIRE(min_sketch.get_num_retained() == (1 << theta_constants::MIN_LG_K));

  // a compacted min-size sketch round trips through serialization
  auto bytes = min_sketch.compact().serialize();
  compact_theta_sketch deserialized = compact_theta_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized.get_num_retained() == min_sketch.get_num_retained());
  REQUIRE(deserialized.get_estimate() == min_sketch.get_estimate());
}

TEST_CASE("theta sketch: non empty no retained keys", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().set_p(0.001f).build();
  update_sketch.update(1);
  //std::cerr << update_sketch.to_string();
  REQUIRE(update_sketch.get_num_retained() == 0);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_estimate() == 0.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(update_sketch.get_upper_bound(1) > 0);

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE(compact_sketch.get_num_retained() == 0);
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_estimate() == 0.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(compact_sketch.get_upper_bound(1) > 0);

  update_sketch.reset();
  REQUIRE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 0.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: single item", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  update_sketch.update(1);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 1.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 1.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 1.0);
  REQUIRE(update_sketch.is_ordered()); // one item is ordered

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() == 1.0);
  REQUIRE(compact_sketch.get_estimate() == 1.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 1.0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 1.0);
  REQUIRE(compact_sketch.is_ordered());

  // single item is forced to be ordered
  REQUIRE(update_sketch.compact(false).is_ordered());
}

TEST_CASE("theta sketch: compact with trim, all four cases", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 8000; i++) update_sketch.update(i);
  const uint32_t k = 1 << theta_constants::DEFAULT_LG_K;
  // an update sketch retains more than k between rebuilds, so trimming has work to do
  REQUIRE(update_sketch.get_num_retained() > k);
  const uint32_t retained_before = update_sketch.get_num_retained();
  const uint64_t theta_before = update_sketch.get_theta64();

  // the trimmed result is what trim() + compact() would produce
  update_theta_sketch trimmed = update_sketch;
  trimmed.trim();
  compact_theta_sketch expected = trimmed.compact(true);
  const std::vector<uint64_t> expected_entries(expected.begin(), expected.end());

  // case 1: ordered, not trimmed (the default) keeps every retained entry
  compact_theta_sketch c1 = update_sketch.compact(true, false);
  REQUIRE(c1.is_ordered());
  REQUIRE(c1.get_num_retained() == retained_before);
  REQUIRE(c1.get_theta64() == theta_before);
  REQUIRE(std::is_sorted(c1.begin(), c1.end()));

  // case 2: unordered, not trimmed
  compact_theta_sketch c2 = update_sketch.compact(false, false);
  REQUIRE_FALSE(c2.is_ordered());
  REQUIRE(c2.get_num_retained() == retained_before);
  REQUIRE(c2.get_theta64() == theta_before);

  // case 3: ordered and trimmed
  compact_theta_sketch c3 = update_sketch.compact(true, true);
  REQUIRE(c3.is_ordered());
  REQUIRE(c3.get_num_retained() == k);
  REQUIRE(c3.get_theta64() < theta_before); // new theta is the k-th smallest hash
  REQUIRE(c3.get_theta64() == expected.get_theta64());
  REQUIRE(std::is_sorted(c3.begin(), c3.end()));
  REQUIRE(std::vector<uint64_t>(c3.begin(), c3.end()) == expected_entries);
  // every retained hash is strictly below the new theta
  for (auto h: c3) REQUIRE(h < c3.get_theta64());

  // case 4: unordered and trimmed: same set and theta, no sort
  compact_theta_sketch c4 = update_sketch.compact(false, true);
  REQUIRE_FALSE(c4.is_ordered());
  REQUIRE(c4.get_num_retained() == k);
  REQUIRE(c4.get_theta64() == expected.get_theta64());
  std::vector<uint64_t> c4_entries(c4.begin(), c4.end());
  std::sort(c4_entries.begin(), c4_entries.end());
  REQUIRE(c4_entries == expected_entries);

  // the source sketch must be untouched by any of the four
  REQUIRE(update_sketch.get_num_retained() == retained_before);
  REQUIRE(update_sketch.get_theta64() == theta_before);
}

TEST_CASE("theta sketch: compact with trim widens the bounds in estimation mode", "[theta_sketch]") {
  // trimming is lossy even when the source is already estimating: it discards
  // retained entries, and the relative error scales with 1 / sqrt(retained)
  const uint32_t k = 1 << theta_constants::DEFAULT_LG_K;
  update_theta_sketch sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 40000; i++) sketch.update(i);
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() > k);

  compact_theta_sketch plain = sketch.compact(true, false);
  compact_theta_sketch trimmed = sketch.compact(true, true);
  REQUIRE(trimmed.get_num_retained() == k);
  REQUIRE(plain.get_num_retained() > trimmed.get_num_retained());

  // fewer retained entries -> strictly wider confidence interval
  const double plain_width = plain.get_upper_bound(2) - plain.get_lower_bound(2);
  const double trimmed_width = trimmed.get_upper_bound(2) - trimmed.get_lower_bound(2);
  REQUIRE(trimmed_width > plain_width);
}

TEST_CASE("theta sketch: compact with trim converts exact mode to estimation", "[theta_sketch]") {
  // an update sketch can retain far more than k entries while still in exact mode:
  // nothing has been evicted yet, so theta is still 1.0 and the count is exact
  const uint32_t k = 1 << theta_constants::DEFAULT_LG_K;
  update_theta_sketch sketch = update_theta_sketch::builder().build();
  const int n = 5000;
  for (int i = 0; i < n; i++) sketch.update(i);
  REQUIRE(sketch.get_num_retained() > k);
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_theta() == 1.0);

  // not trimming keeps every entry and the exact count
  compact_theta_sketch exact = sketch.compact(true, false);
  REQUIRE_FALSE(exact.is_estimation_mode());
  REQUIRE(exact.get_num_retained() == static_cast<uint32_t>(n));
  REQUIRE(exact.get_estimate() == Approx(n));

  // trimming is lossy: it discards real data, lowers theta below 1.0 and the
  // result is an estimate carrying error where the source held an exact count
  compact_theta_sketch trimmed = sketch.compact(true, true);
  REQUIRE(trimmed.is_estimation_mode());
  REQUIRE(trimmed.get_num_retained() == k);
  REQUIRE(trimmed.get_theta() < 1.0);
  REQUIRE(trimmed.get_estimate() != Approx(n));
  // the estimate is still sound: n must lie inside the 3-sigma bounds
  REQUIRE(trimmed.get_lower_bound(3) <= n);
  REQUIRE(trimmed.get_upper_bound(3) >= n);
}

TEST_CASE("theta sketch: compact with trim, empty and below k", "[theta_sketch]") {
  // empty: trimming changes nothing
  update_theta_sketch empty_sketch = update_theta_sketch::builder().build();
  compact_theta_sketch empty_result = empty_sketch.compact(true, true);
  REQUIRE(empty_result.is_empty());
  REQUIRE(empty_result.get_num_retained() == 0);
  REQUIRE(empty_result.is_ordered());

  // below k: nothing to trim, theta and entries are preserved
  update_theta_sketch small = update_theta_sketch::builder().build();
  for (int i = 0; i < 100; i++) small.update(i);
  REQUIRE_FALSE(small.is_estimation_mode());

  compact_theta_sketch small_result = small.compact(true, true);
  REQUIRE_FALSE(small_result.is_estimation_mode());
  REQUIRE(small_result.get_num_retained() == 100);
  REQUIRE(small_result.get_theta64() == small.get_theta64());
  REQUIRE(small_result.get_estimate() == Approx(100.0));
  REQUIRE(std::is_sorted(small_result.begin(), small_result.end()));
  REQUIRE_FALSE(small.compact(false, true).is_ordered());
}

TEST_CASE("theta sketch: resize exact", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 2000; i++) update_sketch.update(i);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 2000.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 2000.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 2000.0);
  REQUIRE_FALSE(update_sketch.is_ordered());

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() == 1.0);
  REQUIRE(compact_sketch.get_estimate() == 2000.0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 2000.0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 2000.0);
  REQUIRE(compact_sketch.is_ordered());

  update_sketch.reset();
  REQUIRE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 0.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0.0);
  REQUIRE(update_sketch.is_ordered());

}

TEST_CASE("theta sketch: estimation", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().set_resize_factor(update_theta_sketch::resize_factor::X1).build();
  const int n = 8000;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  //std::cerr << update_sketch.to_string();
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() < 1.0);
  REQUIRE(update_sketch.get_estimate() == Approx((double) n).margin(n * 0.01));
  REQUIRE(update_sketch.get_lower_bound(1) < n);
  REQUIRE(update_sketch.get_upper_bound(1) > n);

  const uint32_t k = 1 << theta_constants::DEFAULT_LG_K;
  REQUIRE(update_sketch.get_num_retained() >= k);
  update_sketch.trim();
  REQUIRE(update_sketch.get_num_retained() == k);

  compact_theta_sketch compact_sketch = update_sketch.compact();
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE(compact_sketch.is_ordered());
  REQUIRE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_theta() < 1.0);
  REQUIRE(compact_sketch.get_estimate() == Approx((double) n).margin(n * 0.01));
  REQUIRE(compact_sketch.get_lower_bound(1) < n);
  REQUIRE(compact_sketch.get_upper_bound(1) > n);
}

TEST_CASE("theta sketch: deserialize compact v1 empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java_v1.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: deserialize compact v2 empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java_v2.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: deserialize compact v1 estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java_v1.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.is_ordered());
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: deserialize compact v2 estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java_v2.sk", std::ios::binary);
  auto sketch = compact_theta_sketch::deserialize(is);
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
  REQUIRE(sketch.is_ordered());
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto& key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: serialize deserialize stream and bytes equivalence", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  auto compact_sketch = update_sketch.compact();
  compact_sketch.serialize(s);
  auto bytes = compact_sketch.serialize();
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellp()));
  REQUIRE(bytes.size() == compact_sketch.get_serialized_size_bytes());
  for (size_t i = 0; i < bytes.size(); ++i) {
    REQUIRE(((char*)bytes.data())[i] == (char)s.get());
  }

  s.seekg(0); // rewind
  compact_theta_sketch deserialized_sketch1 = compact_theta_sketch::deserialize(s);
  compact_theta_sketch deserialized_sketch2 = compact_theta_sketch::deserialize(bytes.data(), bytes.size());
  REQUIRE(bytes.size() == static_cast<size_t>(s.tellg()));
  REQUIRE(deserialized_sketch2.is_empty() == deserialized_sketch1.is_empty());
  REQUIRE(deserialized_sketch2.is_ordered() == deserialized_sketch1.is_ordered());
  REQUIRE(deserialized_sketch2.get_num_retained() == deserialized_sketch1.get_num_retained());
  REQUIRE(deserialized_sketch2.get_theta() == deserialized_sketch1.get_theta());
  REQUIRE(deserialized_sketch2.get_estimate() == deserialized_sketch1.get_estimate());
  REQUIRE(deserialized_sketch2.get_lower_bound(1) == deserialized_sketch1.get_lower_bound(1));
  REQUIRE(deserialized_sketch2.get_upper_bound(1) == deserialized_sketch1.get_upper_bound(1));
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = deserialized_sketch1.begin();
  for (auto key: deserialized_sketch2) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: deserialize empty buffer overrun", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  auto bytes = update_sketch.compact().serialize();
  REQUIRE(bytes.size() == 8);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("theta sketch: deserialize single item buffer overrun", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  update_sketch.update(1);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("theta sketch: deserialize exact mode buffer overrun", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 1000; ++i) update_sketch.update(i);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 8), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 16), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("theta sketch: deserialize estimation mode buffer overrun", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 10000; ++i) update_sketch.update(i);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 7), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 8), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 16), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), 24), std::out_of_range);
  REQUIRE_THROWS_AS(compact_theta_sketch::deserialize(bytes.data(), bytes.size() - 1), std::out_of_range);
}

TEST_CASE("theta sketch: conversion constructor and wrapped compact", "[theta_sketch]") {
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);

  // unordered
  auto unordered_compact1 = update_sketch.compact(false);
  compact_theta_sketch unordered_compact2(update_sketch, false);
  auto it = unordered_compact1.begin();
  for (auto entry: unordered_compact2) {
    REQUIRE(*it == entry);
    ++it;
  }

  // ordered
  auto ordered_compact1 = update_sketch.compact();
  compact_theta_sketch ordered_compact2(update_sketch, true);
  it = ordered_compact1.begin();
  for (auto entry: ordered_compact2) {
    REQUIRE(*it == entry);
    ++it;
  }

  // wrapped compact
  auto bytes = ordered_compact1.serialize();
  auto ordered_compact3 = wrapped_compact_theta_sketch::wrap(bytes.data(), bytes.size());
  it = ordered_compact1.begin();
  for (auto entry: ordered_compact3) {
    REQUIRE(*it == entry);
    ++it;
  }
  REQUIRE(ordered_compact3.get_estimate() == ordered_compact1.get_estimate());
  REQUIRE(ordered_compact3.get_lower_bound(1) == ordered_compact1.get_lower_bound(1));
  REQUIRE(ordered_compact3.get_upper_bound(1) == ordered_compact1.get_upper_bound(1));
  REQUIRE(ordered_compact3.is_estimation_mode() == ordered_compact1.is_estimation_mode());
  REQUIRE(ordered_compact3.get_theta() == ordered_compact1.get_theta());


  // seed mismatch
  REQUIRE_THROWS_AS(wrapped_compact_theta_sketch::wrap(bytes.data(), bytes.size(), 0), std::invalid_argument);
}

TEST_CASE("theta sketch: wrap compact v1 empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java_v1.sk", std::ios::binary | std::ios::ate);

  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: wrap compact v2 empty from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_empty_from_java_v2.sk", std::ios::binary | std::ios::ate);

  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.get_theta() == 1.0);
  REQUIRE(sketch.get_estimate() == 0.0);
  REQUIRE(sketch.get_lower_bound(1) == 0.0);
  REQUIRE(sketch.get_upper_bound(1) == 0.0);
}

TEST_CASE("theta sketch: wrap compact v1 estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java_v1.sk", std::ios::binary | std::ios::ate);
  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
//  REQUIRE(sketch.is_ordered());       // v1 may not be ordered
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: wrap compact v2 estimation from java", "[theta_sketch]") {
  std::ifstream is;
  is.exceptions(std::ios::failbit | std::ios::badbit);
  is.open(inputPath + "theta_compact_estimation_from_java_v2.sk", std::ios::binary | std::ios::ate);
  std::vector<uint8_t> buf;
  if(is) {
      auto size = is.tellg();
      buf.reserve(size);
      buf.assign(size, 0);
      is.seekg(0, std::ios_base::beg);
      is.read((char*)(buf.data()), buf.size());
  }

  auto sketch = wrapped_compact_theta_sketch::wrap(buf.data(), buf.size());
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE(sketch.is_estimation_mode());
//  REQUIRE(sketch.is_ordered());       // v1 may not be ordered
  REQUIRE(sketch.get_num_retained() == 4342);
  REQUIRE(sketch.get_theta() == Approx(0.531700444213199).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(8166.25234614053).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(7996.956955317471).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(8339.090301078124).margin(1e-10));

  // the same construction process in Java must have produced exactly the same sketch
  update_theta_sketch update_sketch = update_theta_sketch::builder().build();
  const int n = 8192;
  for (int i = 0; i < n; i++) update_sketch.update(i);
  REQUIRE(sketch.get_num_retained() == update_sketch.get_num_retained());
  REQUIRE(sketch.get_theta() == Approx(update_sketch.get_theta()).margin(1e-10));
  REQUIRE(sketch.get_estimate() == Approx(update_sketch.get_estimate()).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(1) == Approx(update_sketch.get_lower_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(1) == Approx(update_sketch.get_upper_bound(1)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(2) == Approx(update_sketch.get_lower_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(2) == Approx(update_sketch.get_upper_bound(2)).margin(1e-10));
  REQUIRE(sketch.get_lower_bound(3) == Approx(update_sketch.get_lower_bound(3)).margin(1e-10));
  REQUIRE(sketch.get_upper_bound(3) == Approx(update_sketch.get_upper_bound(3)).margin(1e-10));
  compact_theta_sketch compact_sketch = update_sketch.compact();
  // the sketches are ordered, so the iteration sequence must match exactly
  auto iter = sketch.begin();
  for (const auto key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: serialize deserialize small compressed", "[theta_sketch]") {
  auto update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 10; i++) update_sketch.update(i);
  auto compact_sketch = update_sketch.compact();

  auto bytes = compact_sketch.serialize_compressed();
  REQUIRE(bytes.size() == compact_sketch.get_serialized_size_bytes(true));
  { // deserialize bytes
    auto deserialized_sketch = compact_theta_sketch::deserialize(bytes.data(), bytes.size());
    REQUIRE(deserialized_sketch.get_num_retained() == compact_sketch.get_num_retained());
    REQUIRE(deserialized_sketch.get_theta() == compact_sketch.get_theta());
    auto iter = deserialized_sketch.begin();
    for (const auto key: compact_sketch) {
      REQUIRE(*iter == key);
      ++iter;
    }
  }
  { // wrap bytes
    auto wrapped_sketch = wrapped_compact_theta_sketch::wrap(bytes.data(), bytes.size());
    REQUIRE(wrapped_sketch.get_num_retained() == compact_sketch.get_num_retained());
    REQUIRE(wrapped_sketch.get_theta() == compact_sketch.get_theta());
    auto iter = wrapped_sketch.begin();
    for (const auto key: compact_sketch) {
      REQUIRE(*iter == key);
      ++iter;
    }
  }

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  compact_sketch.serialize_compressed(s);
  REQUIRE(static_cast<size_t>(s.tellp()) == compact_sketch.get_serialized_size_bytes(true));
  auto deserialized_sketch = compact_theta_sketch::deserialize(s);
  REQUIRE(deserialized_sketch.get_num_retained() == compact_sketch.get_num_retained());
  REQUIRE(deserialized_sketch.get_theta() == compact_sketch.get_theta());
  auto iter = deserialized_sketch.begin();
  for (const auto key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

TEST_CASE("theta sketch: serialize deserialize compressed", "[theta_sketch]") {
  auto update_sketch = update_theta_sketch::builder().build();
  for (int i = 0; i < 10000; i++) update_sketch.update(i);
  auto compact_sketch = update_sketch.compact();

  auto bytes = compact_sketch.serialize_compressed();
  REQUIRE(bytes.size() == compact_sketch.get_serialized_size_bytes(true));
  { // deserialize bytes
    auto deserialized_sketch = compact_theta_sketch::deserialize(bytes.data(), bytes.size());
    REQUIRE(deserialized_sketch.get_num_retained() == compact_sketch.get_num_retained());
    REQUIRE(deserialized_sketch.get_theta() == compact_sketch.get_theta());
    auto iter = deserialized_sketch.begin();
    for (const auto key: compact_sketch) {
      REQUIRE(*iter == key);
      ++iter;
    }
  }
  { // wrap bytes
    auto wrapped_sketch = wrapped_compact_theta_sketch::wrap(bytes.data(), bytes.size());
    REQUIRE(wrapped_sketch.get_num_retained() == compact_sketch.get_num_retained());
    REQUIRE(wrapped_sketch.get_theta() == compact_sketch.get_theta());
    auto iter = wrapped_sketch.begin();
    for (const auto key: compact_sketch) {
      REQUIRE(*iter == key);
      ++iter;
    }
  }

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  compact_sketch.serialize_compressed(s);
  REQUIRE(static_cast<size_t>(s.tellp()) == compact_sketch.get_serialized_size_bytes(true));
  auto deserialized_sketch = compact_theta_sketch::deserialize(s);
  REQUIRE(deserialized_sketch.get_num_retained() == compact_sketch.get_num_retained());
  REQUIRE(deserialized_sketch.get_theta() == compact_sketch.get_theta());
  auto iter = deserialized_sketch.begin();
  for (const auto key: compact_sketch) {
    REQUIRE(*iter == key);
    ++iter;
  }
}

// The sketch reaches capacity for the first time at 2 * K * 15/16,
// but at that point it is still in exact mode, so the serialized size is not the maximum
// (theta in not serialized in the exact mode).
// So we need to catch the second time, but some updates will be ignored in the estimation mode,
// so we update more than enough times keeping track of the maximum.
// Potentially the exact number of updates to reach the peak can be figured out given this particular sequence,
// but not assuming that might be even better (say, in case we change the load factor or hash function
// or just out of principle not to rely on implementation details too much).
TEST_CASE("max serialized size", "[theta_sketch]") {
  const uint8_t lg_k = 10;
  auto sketch = update_theta_sketch::builder().set_lg_k(lg_k).build();
  int value = 0;

  // this will go over the first peak, which is not the highest
  for (int i = 0; i < (1 << lg_k) * 2; ++i) sketch.update(value++);

  // this will to over the second peak keeping track of the max size
  size_t max_size_bytes = 0;
  for (int i = 0; i < (1 << lg_k) * 2; ++i) {
    sketch.update(value++);
    auto bytes = sketch.compact().serialize();
    max_size_bytes = std::max(max_size_bytes, bytes.size());
  }
  REQUIRE(max_size_bytes == compact_theta_sketch::get_max_serialized_size_bytes(lg_k));
}

} /* namespace datasketches */
