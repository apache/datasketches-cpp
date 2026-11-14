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
#include <vector>

#include "tdigest.hpp"
#include "test_allocator.hpp"

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

using tdigest_test_alloc = tdigest<double, test_allocator<double>>;
using tdigest_failing = tdigest<double, failing_allocator<double>>;

const uint16_t K = 100;

double value(int i) {
  // from 100000 on, values extend past both ends of the earlier ones (sources in the merge tests)
  if (i >= 100000) return (i % 2 == 0 ? 1.0 : -1.0) * i;
  // not monotonic, so that merges interleave
  return static_cast<double>((i * 7919) % 10007);
}

tdigest_failing make_tdigest(int first, int count, allocation_control& control) {
  tdigest_failing td(K, failing_allocator<double>(control));
  for (int i = first; i < first + count; ++i) td.update(value(i));
  return td;
}

// the complete state, including the buffered values
std::vector<uint8_t> state(const tdigest_failing& td) {
  const auto bytes = td.serialize(0, true);
  return std::vector<uint8_t>(bytes.begin(), bytes.end());
}

// the t-Digest must still work: query it, update it, merge into it, and round-trip it
void check_usable(tdigest_failing& td, allocation_control& control) {
  control.disarm();
  const uint64_t weight = td.get_total_weight();
  td.update(-1);
  REQUIRE(td.get_total_weight() == weight + 1);
  REQUIRE(td.get_min_value() == -1);
  REQUIRE(td.get_rank(-1) <= 0.5);
  td.merge(make_tdigest(0, 500, control));
  REQUIRE(td.get_total_weight() == weight + 501);
  const auto bytes = td.serialize(0, true);
  const auto copy = tdigest_failing::deserialize(bytes.data(), bytes.size(), failing_allocator<double>(control));
  REQUIRE(copy.get_total_weight() == td.get_total_weight());
}

} // namespace

TEST_CASE("tdigest allocation: reserve allocates the full storage up front", "[tdigest]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    tdigest_test_alloc td(K, test_allocator<double>(0));
    const long long empty_bytes = test_allocator_total_bytes;

    td.reserve();
    const long long reserved_bytes = test_allocator_total_bytes;
    REQUIRE(reserved_bytes > empty_bytes);

    td.reserve(); // already allocated
    REQUIRE(test_allocator_total_bytes == reserved_bytes);

    // compression allocates a temporary vector, but the storage never grows
    for (int i = 0; i < 100000; ++i) td.update(value(i));
    REQUIRE(test_allocator_total_bytes == reserved_bytes);
    REQUIRE(td.get_total_weight() == 100000);

    // the same results as without reserve()
    tdigest_test_alloc unreserved(K, test_allocator<double>(0));
    for (int i = 0; i < 100000; ++i) unreserved.update(value(i));
    REQUIRE(td.serialize(0, true) == unreserved.serialize(0, true));
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("tdigest allocation: reserve after updates does not change the contents", "[tdigest]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    tdigest_test_alloc td(K, test_allocator<double>(0));
    for (int i = 0; i < 1000; ++i) td.update(value(i));
    const auto bytes = td.serialize(0, true);
    td.reserve();
    REQUIRE(td.serialize(0, true) == bytes);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("tdigest allocation: failed reserve", "[tdigest]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    for (long fail_at: {0, 1}) { // centroids, then the buffer
      tdigest_failing td = make_tdigest(0, 10, control);
      const auto before = state(td);
      control.arm(fail_at);
      REQUIRE_THROWS_AS(td.reserve(), std::bad_alloc);
      REQUIRE(state(td) == before);
      check_usable(td, control);
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("tdigest allocation: failed first update leaves the t-Digest unchanged", "[tdigest]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    tdigest_failing td(K, failing_allocator<double>(control));
    const auto before = state(td);
    control.arm(0);
    REQUIRE_THROWS_AS(td.update(1), std::bad_alloc);
    REQUIRE(td.is_empty());
    REQUIRE(state(td) == before);

    control.disarm();
    td.update(2);
    REQUIRE(td.get_total_weight() == 1);
    REQUIRE(td.get_min_value() == 2); // nothing left over from the failed update
    REQUIRE(td.get_max_value() == 2);
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

// Fail each allocation made by a stream of updates in turn. The update that fails must leave the
// t-Digest exactly as the updates before it left it, so it must equal one built from those updates.
TEST_CASE("tdigest allocation: failure at each allocation of the updates", "[tdigest]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    const int n = 5000;
    for (long fail_at = 0; ; ++fail_at) {
      tdigest_failing td(K, failing_allocator<double>(control));
      control.arm(fail_at);
      int updated = 0;
      try {
        for (; updated < n; ++updated) td.update(value(updated));
        control.disarm();
        REQUIRE(fail_at > 0);
        break;
      } catch (const std::bad_alloc&) {
        control.disarm();
        CAPTURE(fail_at, updated);
        REQUIRE(state(td) == state(make_tdigest(0, updated, control)));
        check_usable(td, control);
      }
    }
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

// Fail each allocation of target.merge(source) in turn: a failed merge leaves both unchanged.
TEST_CASE("tdigest allocation: failure at each allocation of a merge", "[tdigest]") {
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    allocation_control control;
    long failures_tested = 0;
    for (int target_n: {0, 5, 1000, 20000}) {
      for (int source_n: {5, 1000, 20000}) {
        CAPTURE(target_n, source_n);
        const tdigest_failing source = make_tdigest(100000, source_n, control);
        const auto source_before = state(source);
        for (long fail_at = 0; ; ++fail_at) {
          tdigest_failing target = make_tdigest(0, target_n, control);
          const auto target_before = state(target);
          control.arm(fail_at);
          try {
            target.merge(source);
            control.disarm();
            REQUIRE(target.get_total_weight() == static_cast<uint64_t>(target_n + source_n));
            break;
          } catch (const std::bad_alloc&) {
            ++failures_tested;
            REQUIRE(state(target) == target_before);
            REQUIRE(state(source) == source_before);
            check_usable(target, control);
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
