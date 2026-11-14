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
#include <new>
#include <string>
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
  uint32_t num_items = 0;
  for (const auto pair: sketch) {
    total_weight += pair.second;
    ++num_items;
  }
  REQUIRE(total_weight == sketch.get_n());
  REQUIRE(num_items == sketch.get_num_retained());
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
    auto deserialized = req_double_test_sketch::deserialize(b.data(), b.size(), serde<double>(),
        std::less<double>(), test_allocator<double>(0));
    REQUIRE(deserialized.is_empty());
    deserialized.update(1);
    REQUIRE(deserialized.get_n() == 1);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
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

} /* namespace datasketches */
