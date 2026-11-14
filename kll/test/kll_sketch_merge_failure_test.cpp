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

#include <kll_sketch.hpp>
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

using sketch_type = kll_sketch<std::string, std::less<std::string>, failing_allocator<std::string>>;

std::string item(int i) {
  // long enough to be allocated outside the string object
  return std::string(32, 'x') + std::to_string(i);
}

sketch_type make_sketch(uint16_t k, int first, int count, allocation_control& control) {
  sketch_type sketch(k, std::less<std::string>(), failing_allocator<std::string>(control));
  for (int i = first; i < first + count; ++i) sketch.update(item(i));
  return sketch;
}

// the sketch must still work: query it, update it, merge into it, and round-trip it
void check_usable(sketch_type& sketch, allocation_control& control) {
  control.disarm();
  const uint64_t n = sketch.get_n();
  sketch.update(item(-1));
  REQUIRE(sketch.get_n() == n + 1);
  REQUIRE(sketch.get_min_item() <= item(-1));
  REQUIRE(sketch.get_quantile(0.5) >= sketch.get_min_item());
  sketch.merge(make_sketch(sketch.get_k(), 0, 500, control));
  REQUIRE(sketch.get_n() == n + 501);
  const auto bytes = sketch.serialize();
  const auto copy = sketch_type::deserialize(bytes.data(), bytes.size(), serde<std::string>(),
      std::less<std::string>(), failing_allocator<std::string>(control));
  REQUIRE(copy.get_n() == sketch.get_n());
  REQUIRE(copy.get_num_retained() == sketch.get_num_retained());
}

// Fails each allocation of target.merge(source) in turn, until the merge completes.
// Returns the number of allocation failures that were tested.
template<bool move_source>
long check_merge_failures(uint16_t k, int target_n, int source_n) {
  allocation_control control;
  const sketch_type original_source = make_sketch(k, 100000, source_n, control);
  const auto source_bytes = original_source.serialize();
  for (long fail_at = 0; ; ++fail_at) {
    sketch_type target = make_sketch(k, 0, target_n, control);
    sketch_type source(original_source);
    const uint64_t target_n_before = target.get_n();
    control.arm(fail_at);
    try {
      if (move_source) target.merge(std::move(source)); else target.merge(source);
      control.disarm();
      REQUIRE(target.get_n() == target_n_before + source.get_n());
      return fail_at;
    } catch (const std::bad_alloc&) {
      check_usable(target, control);
      if (!move_source) REQUIRE(source.serialize() == source_bytes);
    }
  }
}

} // namespace

TEST_CASE("kll sketch: merge failure at each allocation", "[kll_sketch]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  // target sizes: empty, exact mode, estimation mode with several levels
  // source sizes: exact mode, estimation mode with fewer, the same and more levels than the target
  long failures_tested = 0;
  for (int target_n: {0, 5, 1000}) {
    for (int source_n: {5, 100, 1000, 20000}) {
      CAPTURE(target_n, source_n);
      failures_tested += check_merge_failures<false>(8, target_n, source_n);
      failures_tested += check_merge_failures<true>(8, target_n, source_n);
    }
  }
  REQUIRE(failures_tested > 0);
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

} /* namespace datasketches */
