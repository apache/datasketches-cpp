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
#include <string>
#include <utility>

#include <kll_sketch.hpp>
#include <test_allocator.hpp>

namespace datasketches {

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

using kll_double_test_sketch = kll_sketch<double, std::less<double>, test_allocator<double>>;
using kll_string_failing_sketch = kll_sketch<std::string, std::less<std::string>, failing_allocator<std::string>>;

static std::string item(int i) {
  // long enough to be allocated outside the string object
  return std::string(32, 'x') + std::to_string(i);
}

static void check_empty(const kll_string_failing_sketch& sketch, const kll_string_failing_sketch& fresh) {
  REQUIRE(sketch.is_empty());
  REQUIRE(sketch.get_n() == 0);
  REQUIRE(sketch.get_num_retained() == 0);
  REQUIRE(sketch.begin() == sketch.end());
  REQUIRE(sketch.serialize() == fresh.serialize());
}

// the sketch must still work: query it, update it, and round-trip it
static void check_usable(kll_string_failing_sketch& sketch, allocation_control& control) {
  control.disarm();
  const uint64_t n = sketch.get_n();
  sketch.update(item(-1));
  REQUIRE(sketch.get_n() == n + 1);
  REQUIRE(sketch.get_min_item() <= item(-1));
  REQUIRE(sketch.get_quantile(0.5) >= sketch.get_min_item());
  const auto bytes = sketch.serialize();
  const auto copy = kll_string_failing_sketch::deserialize(bytes.data(), bytes.size(), serde<std::string>(),
      std::less<std::string>(), failing_allocator<std::string>(control));
  REQUIRE(copy.get_n() == sketch.get_n());
  REQUIRE(copy.get_num_retained() == sketch.get_num_retained());
}

TEST_CASE("kll sketch allocation: reserve allocates item storage up front", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    kll_double_test_sketch sketch(200, std::less<double>(), test_allocator<double>(0));
    const long long empty_bytes = test_allocator_total_bytes;

    sketch.reserve();
    const long long reserved_bytes = test_allocator_total_bytes;
    REQUIRE(reserved_bytes == empty_bytes + static_cast<long long>(200 * sizeof(double)));

    sketch.reserve(); // already allocated
    REQUIRE(test_allocator_total_bytes == reserved_bytes);

    for (int i = 0; i < 200; ++i) sketch.update(i);
    REQUIRE(test_allocator_total_bytes == reserved_bytes); // the updates up to k allocate nothing
    REQUIRE(sketch.get_n() == 200);
    REQUIRE(sketch.get_min_item() == 0);
    REQUIRE(sketch.get_max_item() == 199);

    kll_double_test_sketch target(200, std::less<double>(), test_allocator<double>(0));
    target.reserve();
    target.merge(sketch);
    REQUIRE(target.get_n() == 200);
    REQUIRE(target.get_min_item() == 0);
    REQUIRE(target.get_max_item() == 199);
    REQUIRE(target.get_quantile(0.5) == sketch.get_quantile(0.5));
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("kll sketch allocation: reserve after updates does nothing", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    kll_double_test_sketch sketch(8, std::less<double>(), test_allocator<double>(0));
    for (int i = 0; i < 1000; ++i) sketch.update(i);
    const auto bytes = sketch.serialize();
    const long long allocated = test_allocator_total_bytes;
    sketch.reserve();
    REQUIRE(test_allocator_total_bytes == allocated);
    REQUIRE(sketch.serialize() == bytes);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("kll sketch allocation: failed reserve", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    const failing_allocator<std::string> alloc(control);
    const kll_string_failing_sketch fresh(200, std::less<std::string>(), alloc);
    kll_string_failing_sketch sketch(200, std::less<std::string>(), alloc);
    control.arm(0);
    REQUIRE_THROWS_AS(sketch.reserve(), std::bad_alloc);
    check_empty(sketch, fresh);
    check_usable(sketch, control);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("kll sketch allocation: failed first update leaves the sketch unchanged", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    const failing_allocator<std::string> alloc(control);
    const kll_string_failing_sketch fresh(200, std::less<std::string>(), alloc);
    kll_string_failing_sketch sketch(200, std::less<std::string>(), alloc);
    control.arm(0);
    REQUIRE_THROWS_AS(sketch.update(item(1)), std::bad_alloc);
    check_empty(sketch, fresh);

    control.disarm();
    sketch.update(item(2));
    REQUIRE(sketch.get_n() == 1);
    REQUIRE(sketch.get_min_item() == item(2)); // nothing left over from the failed update
    REQUIRE(sketch.get_max_item() == item(2));
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("kll sketch allocation: failed first merge leaves both sketches unchanged", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    const failing_allocator<std::string> alloc(control);
    const kll_string_failing_sketch fresh(200, std::less<std::string>(), alloc);
    kll_string_failing_sketch source(200, std::less<std::string>(), alloc);
    for (int i = 0; i < 1000; ++i) source.update(item(i)); // estimation mode, several levels
    const auto source_bytes = source.serialize();

    kll_string_failing_sketch target(200, std::less<std::string>(), alloc);
    control.arm(0);
    REQUIRE_THROWS_AS(target.merge(source), std::bad_alloc);
    check_empty(target, fresh);
    REQUIRE(source.serialize() == source_bytes);

    control.arm(0);
    REQUIRE_THROWS_AS(target.merge(std::move(source)), std::bad_alloc);
    check_empty(target, fresh);
    REQUIRE(source.serialize() == source_bytes); // nothing was moved out

    control.disarm();
    target.merge(source);
    REQUIRE(target.get_n() == source.get_n());
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

// Fail each allocation made by a stream of updates in turn: the sketch must stay valid and usable.
TEST_CASE("kll sketch allocation: failure at each allocation of the updates", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    const failing_allocator<std::string> alloc(control);
    const kll_string_failing_sketch fresh(8, std::less<std::string>(), alloc);
    bool completed = false;
    for (long fail_at = 0; !completed; ++fail_at) {
      kll_string_failing_sketch sketch(8, std::less<std::string>(), alloc);
      control.arm(fail_at);
      try {
        for (int i = 0; i < 2000; ++i) sketch.update(item(i));
        completed = true;
        REQUIRE(fail_at > 0);
      } catch (const std::bad_alloc&) {
        if (fail_at == 0) check_empty(sketch, fresh);
        check_usable(sketch, control);
      }
      control.disarm();
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("kll sketch allocation: failed first weighted update leaves the sketch unchanged", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    const failing_allocator<std::string> alloc(control);
    const kll_string_failing_sketch fresh(200, std::less<std::string>(), alloc);
    for (const uint64_t weight: {uint64_t(10), uint64_t(1000)}) { // copies into level zero, and a merge
      kll_string_failing_sketch sketch(200, std::less<std::string>(), alloc);
      control.arm(0);
      REQUIRE_THROWS_AS(sketch.update(item(1), weight), std::bad_alloc);
      check_empty(sketch, fresh);
      control.disarm();
      sketch.update(item(2), weight);
      REQUIRE(sketch.get_n() == weight);
      REQUIRE(sketch.get_min_item() == item(2));
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

// Fail each allocation made by a stream of weighted updates in turn: the sketch must stay valid and usable.
TEST_CASE("kll sketch allocation: failure at each allocation of the weighted updates", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    const failing_allocator<std::string> alloc(control);
    const kll_string_failing_sketch fresh(8, std::less<std::string>(), alloc);
    bool completed = false;
    for (long fail_at = 0; !completed; ++fail_at) {
      kll_string_failing_sketch sketch(8, std::less<std::string>(), alloc);
      control.arm(fail_at);
      try {
        for (int i = 0; i < 300; ++i) sketch.update(item(i), static_cast<uint64_t>(i % 7) * 100 + 1);
        completed = true;
        REQUIRE(fail_at > 0);
      } catch (const std::bad_alloc&) {
        if (fail_at == 0) check_empty(sketch, fresh);
        check_usable(sketch, control);
      }
      control.disarm();
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

namespace {

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
  fragile_item& operator=(fragile_item other) noexcept { std::swap(value, other.value); return *this; }
  ~fragile_item() { if (value != nullptr) test_allocator<int>(0).deallocate(value, 1); }
};

struct fragile_less {
  bool operator()(const fragile_item& a, const fragile_item& b) const { return *a.value < *b.value; }
};

using kll_fragile_sketch = kll_sketch<fragile_item, fragile_less, test_allocator<fragile_item>>;

} // namespace

// Fail each item copy made by a weighted update in turn: nothing may leak,
// and the sketch must stay usable, with no stale sorted view.
TEST_CASE("kll sketch allocation: failure at each item copy of a weighted update", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  for (const uint64_t weight: {uint64_t(10), uint64_t(1000)}) { // copies into level zero, and a merge
    CAPTURE(weight);
    bool completed = false;
    for (long fail_at = 0; !completed; ++fail_at) {
      CAPTURE(fail_at);
      kll_fragile_sketch sketch(200, fragile_less(), test_allocator<fragile_item>(0));
      for (int i = 0; i < 5; ++i) sketch.update(fragile_item(i));
      const fragile_item probe(2);
      const double rank_before = sketch.get_rank(probe); // caches a sorted view
      REQUIRE(rank_before > 0);
      copies_allowed = fail_at;
      try {
        sketch.update(fragile_item(100), weight);
        completed = true;
      } catch (const std::runtime_error&) {}
      copies_allowed = -1;
      REQUIRE(sketch.get_n() >= 5);
      REQUIRE(sketch.get_n() <= 5 + weight);
      // a copy of the sketch builds its sorted view from the current items
      const kll_fragile_sketch copy(sketch);
      REQUIRE(sketch.get_rank(probe) == copy.get_rank(probe));
      sketch.update(fragile_item(-1));
      REQUIRE(*sketch.get_min_item().value == -1);
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

} /* namespace datasketches */
