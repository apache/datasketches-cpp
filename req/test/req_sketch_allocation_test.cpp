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
#include <algorithm>
#include <new>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <req_sketch.hpp>
#include <test_allocator.hpp>

namespace datasketches {

namespace {

// Counts allocations, and throws std::bad_alloc on the allocation numbered fail_at (counting from 0).
struct allocation_control {
  long count = 0;
  long fail_at = -1;
  void arm(long at) { count = 0; fail_at = at; }
  void disarm() { fail_at = -1; }
};

template<typename T>
struct failing_allocator: test_allocator<T> {
  allocation_control* control;
  template<typename U> struct rebind { using other = failing_allocator<U>; };
  explicit failing_allocator(allocation_control& c): test_allocator<T>(0), control(&c) {}
  template<typename U>
  failing_allocator(const failing_allocator<U>& other): test_allocator<T>(0), control(other.control) {}
  T* allocate(std::size_t n, const T* hint = nullptr) {
    if (control->fail_at >= 0 && control->count++ == control->fail_at) throw std::bad_alloc();
    return test_allocator<T>::allocate(n, hint);
  }
};

template<typename T, typename U>
bool operator==(const failing_allocator<T>& a, const failing_allocator<U>& b) { return a.control == b.control; }
template<typename T, typename U>
bool operator!=(const failing_allocator<T>& a, const failing_allocator<U>& b) { return !(a == b); }

using req_double_test_sketch = req_sketch<double, std::less<double>, test_allocator<double>>;
using sketch_type = req_sketch<std::string, std::less<std::string>, failing_allocator<std::string>>;

const uint16_t K = 12;

std::string item(int i) {
  // long enough to be allocated outside the string object
  return std::string(32, 'x') + std::to_string(i);
}

sketch_type make_sketch(int first, int count, allocation_control& control, bool hra = true) {
  sketch_type sketch(K, hra, std::less<std::string>(), failing_allocator<std::string>(control));
  for (int i = first; i < first + count; ++i) sketch.update(item(i));
  return sketch;
}

std::vector<uint8_t> bytes(const sketch_type& sketch) {
  const auto b = sketch.serialize();
  return std::vector<uint8_t>(b.begin(), b.end());
}

// the sketch must be consistent: the weights of the retained items add up to n
void check_consistent(const sketch_type& sketch) {
  uint64_t total_weight = 0;
  uint64_t max_weight = 0;
  uint32_t num_items = 0;
  for (const auto pair: sketch) {
    total_weight += pair.second;
    max_weight = std::max(max_weight, pair.second);
    ++num_items;
  }
  REQUIRE(total_weight == sketch.get_n());
  REQUIRE(num_items == sketch.get_num_retained());
  // no empty level is left above level zero
  REQUIRE(sketch.is_estimation_mode() == (max_weight > 1));
}

// the sketch must still work: query it, update it, merge into it, and round-trip it
void check_usable(sketch_type& sketch, allocation_control& control) {
  control.disarm();
  check_consistent(sketch);
  const uint64_t n = sketch.get_n();
  sketch.update(item(-1));
  REQUIRE(sketch.get_n() == n + 1);
  REQUIRE(sketch.get_min_item() <= item(-1));
  REQUIRE(sketch.get_quantile(0.5) >= sketch.get_min_item());
  sketch.merge(make_sketch(0, 500, control, sketch.is_HRA()));
  REQUIRE(sketch.get_n() == n + 501);
  check_consistent(sketch);
  const auto b = sketch.serialize();
  const auto copy = sketch_type::deserialize(b.data(), b.size(), serde<std::string>(),
      std::less<std::string>(), failing_allocator<std::string>(control));
  REQUIRE(copy.get_n() == sketch.get_n());
  REQUIRE(copy.get_num_retained() == sketch.get_num_retained());
}


using compactor_type = req_compactor<double, std::less<double>, failing_allocator<double>>;

std::vector<uint8_t> bytes(const compactor_type& compactor) {
  std::vector<uint8_t> b(compactor.get_serialized_size_bytes(serde<double>()));
  compactor.serialize(b.data(), b.size(), serde<double>());
  return b;
}

long copies_allowed = -1; // copies left before a copy throws; negative means no limit

// An item whose copy constructor throws once copies_allowed runs out.
// Each item owns memory from test_allocator, so an item that is never destroyed shows up as a leak.
struct fragile_item {
  int* value;
  explicit fragile_item(int v): value(test_allocator<int>(0).allocate(1)) { *value = v; }
  fragile_item(const fragile_item& other): value(nullptr) {
    if (copies_allowed == 0) throw std::runtime_error("copy failed");
    if (copies_allowed > 0) --copies_allowed;
    value = test_allocator<int>(0).allocate(1);
    *value = *other.value;
  }
  fragile_item(fragile_item&& other) noexcept: value(other.value) { other.value = nullptr; }
  fragile_item& operator=(const fragile_item& other) {
    fragile_item copy(other);
    std::swap(value, copy.value);
    return *this;
  }
  fragile_item& operator=(fragile_item&& other) noexcept { std::swap(value, other.value); return *this; }
  ~fragile_item() { if (value != nullptr) test_allocator<int>(0).deallocate(value, 1); }
};

struct fragile_less {
  bool operator()(const fragile_item& a, const fragile_item& b) const { return *a.value < *b.value; }
};

using fragile_sketch = req_sketch<fragile_item, fragile_less, test_allocator<fragile_item>>;

fragile_sketch make_fragile_sketch(int first, int count, bool hra) {
  fragile_sketch sketch(K, hra, fragile_less(), test_allocator<fragile_item>(0));
  for (int i = first; i < first + count; ++i) sketch.update(fragile_item((i * 7919) % 10007));
  return sketch;
}

// the retained items must account for n, and every item must be a live object
void check_consistent(const fragile_sketch& sketch) {
  uint64_t total_weight = 0;
  uint32_t num_items = 0;
  for (const auto pair: sketch) {
    REQUIRE(pair.first.value != nullptr);
    total_weight += pair.second;
    ++num_items;
  }
  REQUIRE(total_weight == sketch.get_n());
  REQUIRE(num_items == sketch.get_num_retained());
  if (!sketch.is_empty()) {
    REQUIRE(*sketch.get_min_item().value <= *sketch.get_max_item().value);
  }
}

} // namespace

TEST_CASE("req sketch allocation: reserve allocates item storage up front", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    for (bool hra: {true, false}) {
      req_double_test_sketch sketch(K, hra, std::less<double>(), test_allocator<double>(0));
      const long long empty_bytes = test_allocator_total_bytes;
      // 2 * nominal capacity of the first compactor: 2 * 2 * 3 sections * k
      const long long storage_bytes = static_cast<long long>(2 * 2 * 3 * K * sizeof(double));

      sketch.reserve();
      const long long reserved_bytes = test_allocator_total_bytes;
      REQUIRE(reserved_bytes == empty_bytes + storage_bytes);

      sketch.reserve(); // already allocated
      REQUIRE(test_allocator_total_bytes == reserved_bytes);

      for (int i = 0; i < 2 * 3 * K - 1; ++i) sketch.update(i); // fills the first compactor up to its nominal capacity
      REQUIRE(test_allocator_total_bytes == reserved_bytes);
      REQUIRE(sketch.get_n() == 2 * 3 * K - 1);
      REQUIRE(sketch.get_min_item() == 0);

      req_double_test_sketch target(K, hra, std::less<double>(), test_allocator<double>(0));
      target.reserve();
      target.merge(sketch);
      REQUIRE(target.get_n() == sketch.get_n());
      REQUIRE(target.get_quantile(0.5) == sketch.get_quantile(0.5));
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("req sketch allocation: an empty sketch has no item storage", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    req_double_test_sketch sketch(K, true, std::less<double>(), test_allocator<double>(0));
    const long long empty_bytes = test_allocator_total_bytes;
    REQUIRE(empty_bytes < static_cast<long long>(2 * 2 * 3 * K * sizeof(double)));

    req_double_test_sketch copy(sketch);
    req_double_test_sketch target(K, true, std::less<double>(), test_allocator<double>(0));
    target.merge(sketch); // merging an empty sketch allocates no item storage
    REQUIRE(test_allocator_total_bytes == 3 * empty_bytes);
    REQUIRE(target.is_empty());

    const auto b = sketch.serialize();
    const long long bytes_before = test_allocator_total_bytes;
    auto deserialized = req_double_test_sketch::deserialize(b.data(), b.size(), serde<double>(),
        std::less<double>(), test_allocator<double>(0));
    REQUIRE(test_allocator_total_bytes - bytes_before == empty_bytes); // no item storage, like a new sketch
    REQUIRE(deserialized.is_empty());
    deserialized.update(1);
    REQUIRE(deserialized.get_n() == 1);

    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    sketch.serialize(s);
    const long long stream_bytes_before = test_allocator_total_bytes;
    auto from_stream = req_double_test_sketch::deserialize(s, serde<double>(), std::less<double>(),
        test_allocator<double>(0));
    REQUIRE(test_allocator_total_bytes - stream_bytes_before == empty_bytes);
    REQUIRE(from_stream.is_empty());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("req compactor allocation: merging an empty compactor allocates no item storage", "[req_sketch]") {
  using compactor_type = req_compactor<double, std::less<double>, test_allocator<double>>;
  for (bool hra: {true, false}) {
    test_allocator_total_bytes = 0;
    test_allocator_net_allocations = 0;
    {
      compactor_type target(hra, 0, K, std::less<double>(), test_allocator<double>(0));
      compactor_type source(hra, 0, K, std::less<double>(), test_allocator<double>(0));
      target.merge(source);
      REQUIRE(test_allocator_total_bytes == 0);
      REQUIRE(target.get_num_items() == 0);
    }
    REQUIRE(test_allocator_net_allocations == 0);
  }
}

TEST_CASE("req sketch: iterating skips levels without items", "[req_sketch]") {
  for (bool hra: {true, false}) {
    req_sketch<double> sketch(K, hra);
    REQUIRE(sketch.begin() == sketch.end());
    for (const auto pair: sketch) { (void) pair; FAIL("an empty sketch has no items"); }
    sketch.reserve();
    REQUIRE(sketch.begin() == sketch.end());

    // a merge can leave level zero without items while higher levels have them
    req_sketch<double> source(K, hra);
    for (int i = 0; i < 10000; ++i) source.update(i);
    req_sketch<double> target(K, hra);
    target.merge(source);
    uint64_t total_weight = 0;
    for (const auto pair: target) total_weight += pair.second;
    REQUIRE(total_weight == target.get_n());
  }
}

TEST_CASE("req sketch allocation: reserve after updates does nothing", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    req_double_test_sketch sketch(K, true, std::less<double>(), test_allocator<double>(0));
    for (int i = 0; i < 1000; ++i) sketch.update(i);
    const auto b = sketch.serialize();
    const long long allocated = test_allocator_total_bytes;
    sketch.reserve();
    REQUIRE(test_allocator_total_bytes == allocated);
    REQUIRE(sketch.serialize() == b);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("req sketch allocation: failed reserve", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    const sketch_type fresh = make_sketch(0, 0, control);
    sketch_type sketch = make_sketch(0, 0, control);
    control.arm(0);
    REQUIRE_THROWS_AS(sketch.reserve(), std::bad_alloc);
    REQUIRE(sketch.is_empty());
    REQUIRE(bytes(sketch) == bytes(fresh));
    check_usable(sketch, control);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("req sketch allocation: failed first update leaves the sketch unchanged", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    for (bool hra: {true, false}) {
      const sketch_type fresh = make_sketch(0, 0, control, hra);
      sketch_type sketch = make_sketch(0, 0, control, hra);
      control.arm(0);
      REQUIRE_THROWS_AS(sketch.update(item(1)), std::bad_alloc);
      REQUIRE(sketch.is_empty());
      REQUIRE(sketch.get_num_retained() == 0);
      REQUIRE(bytes(sketch) == bytes(fresh));

      control.disarm();
      sketch.update(item(2));
      REQUIRE(sketch.get_n() == 1);
      REQUIRE(sketch.get_min_item() == item(2)); // nothing left over from the failed update
      REQUIRE(sketch.get_max_item() == item(2));
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

// Fail each allocation made by a stream of updates in turn: the sketch must stay valid and usable.
TEST_CASE("req sketch allocation: failure at each allocation of the updates", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    for (bool hra: {true, false}) {
      for (long fail_at = 0; ; ++fail_at) {
        sketch_type sketch(K, hra, std::less<std::string>(), failing_allocator<std::string>(control));
        control.arm(fail_at);
        try {
          for (int i = 0; i < 5000; ++i) sketch.update(item(i));
          control.disarm();
          REQUIRE(fail_at > 0);
          break;
        } catch (const std::bad_alloc&) {
          CAPTURE(hra, fail_at);
          control.disarm();
          check_consistent(sketch);
          // it must keep compressing as updates continue: retain no more than a sketch that never failed
          for (int i = 0; i < 20000; ++i) sketch.update(item(i));
          const sketch_type reference = make_sketch(0, static_cast<int>(sketch.get_n()), control, hra);
          REQUIRE(sketch.get_num_retained() <= 2 * reference.get_num_retained());
          check_usable(sketch, control);
        }
      }
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

// Fail each allocation of target.merge(source) in turn: the target must stay valid and usable,
// and a failure of the first allocation, the item storage of an empty target, leaves it unchanged.
TEST_CASE("req sketch allocation: failure at each allocation of a merge", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    long failures_tested = 0;
    for (bool hra: {true, false}) {
      for (int target_n: {0, 5, 1000}) {
        for (int source_n: {5, 1000, 20000}) {
          CAPTURE(hra, target_n, source_n);
          const sketch_type source = make_sketch(100000, source_n, control, hra);
          const auto source_bytes = bytes(source);
          for (long fail_at = 0; ; ++fail_at) {
            sketch_type target = make_sketch(0, target_n, control, hra);
            const bool was_estimation_mode = target.is_estimation_mode();
            control.arm(fail_at);
            try {
              target.merge(source);
              control.disarm();
              REQUIRE(target.get_n() == static_cast<uint64_t>(target_n + source_n));
              break;
            } catch (const std::bad_alloc&) {
              ++failures_tested;
              control.disarm();
              CAPTURE(fail_at);
              REQUIRE(bytes(source) == source_bytes);
              // levels added for the merge must not stay behind empty: if no items were merged,
              // the target has the same levels as before
              if (target.get_n() == static_cast<uint64_t>(target_n)) {
                REQUIRE(target.is_estimation_mode() == was_estimation_mode);
              }
              check_usable(target, control);
            }
          }
        }
      }
    }
    REQUIRE(failures_tested > 0);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

// A compaction that fails to allocate must change neither compactor, and must draw no random bit,
// so that compacting again gives what the compaction would have given without the failure.
TEST_CASE("req compactor allocation: a failed compaction changes nothing", "[req_sketch]") {
  allocation_control control;
  long failures_tested = 0;
  for (bool hra: {true, false}) {
    CAPTURE(hra);
    random_utils::random_bit.seed(3);
    compactor_type level0(hra, 0, K, std::less<double>(), failing_allocator<double>(control));
    compactor_type level1(hra, 1, K, std::less<double>(), failing_allocator<double>(control));
    int value = 0;
    // enough compactions to reach both coin cases and a change of the sections
    for (int round = 0; round < 12; ++round) {
      CAPTURE(round);
      while (level0.get_num_items() < level0.get_nom_capacity()) level0.append(static_cast<double>((value++ * 7919) % 10007));
      level0.sort();
      const compactor_type level0_before(level0);
      const compactor_type level1_before(level1);
      const auto bits_before = random_utils::random_bit;
      bool completed = false;
      for (long fail_at = 0; !completed; ++fail_at) {
        CAPTURE(fail_at);
        control.arm(fail_at);
        try {
          level0.compact(level1);
          completed = true;
        } catch (const std::bad_alloc&) {
          ++failures_tested;
        }
        control.disarm();
        if (!completed) {
          REQUIRE(bytes(level0) == bytes(level0_before));
          REQUIRE(bytes(level1) == bytes(level1_before));
          REQUIRE(random_utils::random_bit == bits_before);
        }
      }
      // the same as the compaction without failures, from the same random state
      const auto bits_after = random_utils::random_bit;
      compactor_type expected0(level0_before);
      compactor_type expected1(level1_before);
      random_utils::random_bit = bits_before;
      expected0.compact(expected1);
      REQUIRE(random_utils::random_bit == bits_after);
      REQUIRE(bytes(level0) == bytes(expected0));
      REQUIRE(bytes(level1) == bytes(expected1));
    }
  }
  REQUIRE(failures_tested > 0);
}

// Fail each item copy made by the updates in turn: a failed update must leave the sketch as it was.
TEST_CASE("req sketch allocation: failure at each item copy of the updates", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  for (bool hra: {true, false}) {
    CAPTURE(hra);
    fragile_sketch sketch(K, hra, fragile_less(), test_allocator<fragile_item>(0));
    for (int i = 0; i < 300; ++i) {
      const fragile_item item((i * 7919) % 10007);
      const uint64_t n_before = sketch.get_n();
      const uint32_t num_retained_before = sketch.get_num_retained();
      const int min_before = sketch.is_empty() ? 0 : *sketch.get_min_item().value;
      const int max_before = sketch.is_empty() ? 0 : *sketch.get_max_item().value;
      bool completed = false;
      for (long fail_at = 0; !completed; ++fail_at) {
        CAPTURE(i, fail_at);
        copies_allowed = fail_at;
        try {
          sketch.update(item);
          completed = true;
        } catch (const std::runtime_error&) {}
        copies_allowed = -1;
        if (!completed) {
          REQUIRE(sketch.get_n() == n_before);
          REQUIRE(sketch.get_num_retained() == num_retained_before);
          if (n_before > 0) {
            REQUIRE(*sketch.get_min_item().value == min_before);
            REQUIRE(*sketch.get_max_item().value == max_before);
          }
        }
        check_consistent(sketch);
      }
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

// Fail each item copy made by merging from a const sketch in turn: the target must stay valid and usable.
TEST_CASE("req sketch allocation: failure at each item copy of a merge", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  for (bool hra: {true, false}) {
    for (int target_n: {0, 5, 1000}) {
      for (int source_n: {5, 1000}) {
        CAPTURE(hra, target_n, source_n);
        const fragile_sketch source = make_fragile_sketch(100000, source_n, hra);
        bool completed = false;
        for (long fail_at = 0; !completed; ++fail_at) {
          CAPTURE(fail_at);
          fragile_sketch target = make_fragile_sketch(0, target_n, hra);
          copies_allowed = fail_at;
          try {
            target.merge(source);
            completed = true;
          } catch (const std::runtime_error&) {}
          copies_allowed = -1;
          REQUIRE(target.get_n() >= static_cast<uint64_t>(target_n));
          REQUIRE(target.get_n() <= static_cast<uint64_t>(target_n + source_n));
          if (completed) REQUIRE(target.get_n() == static_cast<uint64_t>(target_n + source_n));
          check_consistent(target);
          check_consistent(source);
          REQUIRE(source.get_n() == static_cast<uint64_t>(source_n));
          // the target stays usable
          target.merge(source);
          target.update(fragile_item(-1));
          check_consistent(target);
          REQUIRE(*target.get_min_item().value == -1);
        }
      }
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("req sketch allocation: failure at each item copy of the copy constructor", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  for (bool hra: {true, false}) {
    const fragile_sketch sketch = make_fragile_sketch(0, 1000, hra);
    bool completed = false;
    for (long fail_at = 0; !completed; ++fail_at) {
      CAPTURE(hra, fail_at);
      copies_allowed = fail_at;
      try {
        const fragile_sketch copy(sketch);
        completed = true;
        copies_allowed = -1;
        REQUIRE(copy.get_n() == sketch.get_n());
        REQUIRE(copy.get_num_retained() == sketch.get_num_retained());
      } catch (const std::runtime_error&) {}
      copies_allowed = -1;
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("req sketch allocation: converting constructor that rejects the order releases the items", "[req_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    req_double_test_sketch sketch(K, true, std::less<double>(), test_allocator<double>(0));
    for (int i = 0; i < 1000; ++i) sketch.update(i);
    REQUIRE(sketch.is_estimation_mode());
    using descending_sketch = req_sketch<double, std::greater<double>, test_allocator<double>>;
    REQUIRE_THROWS_AS(descending_sketch(sketch, std::greater<double>(), test_allocator<double>(0)), std::logic_error);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}


} /* namespace datasketches */
