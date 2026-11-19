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
#include <stdexcept>
#include <utility>

#include <quantiles_sketch.hpp>
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

using quantiles_failing_sketch = quantiles_sketch<double, std::less<double>, failing_allocator<double>>;

quantiles_failing_sketch make_sketch(uint16_t k, int n, int offset, allocation_control& control) {
  quantiles_failing_sketch sketch(k, std::less<double>(), failing_allocator<double>(control));
  for (int i = 0; i < n; ++i) sketch.update(static_cast<double>((i * 7919 + offset) % 10007));
  return sketch;
}

// serialize() sorts the base buffer, which changes how later updates fill it,
// so the bytes of a sketch under test are taken from a copy
template<typename S>
typename S::vector_bytes bytes_of(const S& sketch) {
  S copy(sketch);
  return copy.serialize();
}

void seed_random(uint32_t seed) {
  random_utils::random_bit.seed(seed);
  random_utils::rand.seed(seed);
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

using quantiles_fragile_sketch = quantiles_sketch<fragile_item, fragile_less, test_allocator<fragile_item>>;

quantiles_fragile_sketch make_fragile_sketch(uint16_t k, int n, int offset) {
  quantiles_fragile_sketch sketch(k, fragile_less(), test_allocator<fragile_item>(0));
  for (int i = 0; i < n; ++i) sketch.update(fragile_item((i * 7919 + offset) % 10007));
  return sketch;
}

// the retained items must account for n, and every item must be a live object
void check_consistent(const quantiles_fragile_sketch& sketch) {
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

struct merge_config { uint16_t k; int n; uint16_t other_k; int other_n; };

// every path through merge(): same k, a target with larger or smaller k,
// an exact or empty target, and an exact source
const merge_config merge_configs[] = {
  {16, 500, 16, 300},
  {32, 500, 16, 300},
  {16, 500, 32, 300},
  {16, 10, 16, 300},
  {32, 10, 16, 300},
  {16, 0, 16, 300},
  {16, 500, 16, 10},
  {16, 0, 16, 10},
};

} // namespace

TEST_CASE("quantiles sketch allocation: failure at each allocation of the updates", "[quantiles_sketch]") {
  allocation_control control;
  const uint16_t k = 16;
  const int n = 1000;
  bool completed = false;
  for (long fail_at = 0; !completed; ++fail_at) {
    CAPTURE(fail_at);
    seed_random(1);
    quantiles_failing_sketch sketch(k, std::less<double>(), failing_allocator<double>(control));
    int failed_item = -1;
    control.arm(fail_at);
    for (int i = 0; i < n; ++i) {
      try {
        sketch.update(i);
      } catch (const std::bad_alloc&) {
        failed_item = i;
        break;
      }
    }
    control.disarm();
    if (failed_item < 0) {
      completed = true;
    } else {
      REQUIRE(sketch.get_n() == static_cast<uint64_t>(failed_item));
      for (int i = failed_item + 1; i < n; ++i) sketch.update(i); // keep going, as an application would
    }
    // the same as a sketch that never saw the failed item
    seed_random(1);
    quantiles_failing_sketch expected(k, std::less<double>(), failing_allocator<double>(control));
    for (int i = 0; i < n; ++i) if (i != failed_item) expected.update(i);
    REQUIRE(sketch.serialize() == expected.serialize());
  }
}

TEST_CASE("quantiles sketch allocation: failure at each allocation of a merge", "[quantiles_sketch]") {
  allocation_control control;
  for (const merge_config& c: merge_configs) {
    for (const bool move_other: {false, true}) {
      CAPTURE(c.k, c.n, c.other_k, c.other_n, move_other);
      seed_random(2);
      auto expected = make_sketch(c.k, c.n, 1, control);
      expected.merge(make_sketch(c.other_k, c.other_n, 2, control));
      const auto expected_bytes = expected.serialize();
      bool completed = false;
      for (long fail_at = 0; !completed; ++fail_at) {
        CAPTURE(fail_at);
        seed_random(2);
        auto sketch = make_sketch(c.k, c.n, 1, control);
        auto other = make_sketch(c.other_k, c.other_n, 2, control);
        const auto other_copy = other; // for a retry: a copy draws no random bits
        const auto bytes_before = bytes_of(sketch);
        control.arm(fail_at);
        try {
          if (move_other) sketch.merge(std::move(other)); else sketch.merge(other);
          completed = true;
        } catch (const std::bad_alloc&) {}
        control.disarm();
        if (!completed) {
          REQUIRE(bytes_of(sketch) == bytes_before); // unchanged
          // and still usable: the merge can be repeated
          sketch.merge(other_copy);
        }
        REQUIRE(sketch.serialize() == expected_bytes);
      }
    }
  }
}

TEST_CASE("quantiles sketch allocation: failure at each item copy of the updates", "[quantiles_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    quantiles_fragile_sketch sketch(4, fragile_less(), test_allocator<fragile_item>(0));
    for (int i = 0; i < 200; ++i) {
      const fragile_item item((i * 7919) % 10007);
      const uint64_t n_before = sketch.get_n();
      // fail each copy that this update makes in turn
      bool completed = false;
      for (long fail_at = 0; !completed; ++fail_at) {
        copies_allowed = fail_at;
        try {
          sketch.update(item);
          completed = true;
        } catch (const std::runtime_error&) {}
        copies_allowed = -1;
        REQUIRE(sketch.get_n() == n_before + (completed ? 1 : 0));
        check_consistent(sketch);
      }
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("quantiles sketch allocation: failure at each item copy of a merge", "[quantiles_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  for (const merge_config& c: merge_configs) {
    CAPTURE(c.k, c.n, c.other_k, c.other_n);
    const auto other = make_fragile_sketch(c.other_k, c.other_n, 2);
    bool completed = false;
    for (long fail_at = 0; !completed; ++fail_at) {
      CAPTURE(fail_at);
      auto sketch = make_fragile_sketch(c.k, c.n, 1);
      const uint64_t n_before = sketch.get_n();
      copies_allowed = fail_at;
      try {
        sketch.merge(other);
        completed = true;
      } catch (const std::runtime_error&) {}
      copies_allowed = -1;
      REQUIRE(sketch.get_n() >= n_before);
      REQUIRE(sketch.get_n() <= n_before + other.get_n());
      if (completed) REQUIRE(sketch.get_n() == n_before + other.get_n());
      check_consistent(sketch);
      check_consistent(other);
      REQUIRE(other.get_n() == static_cast<uint64_t>(c.other_n));
      // the sketch stays usable
      sketch.merge(other);
      sketch.update(fragile_item(-1));
      check_consistent(sketch);
      REQUIRE(*sketch.get_min_item().value == -1);
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

} /* namespace datasketches */
