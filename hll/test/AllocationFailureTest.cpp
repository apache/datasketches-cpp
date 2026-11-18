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

#include "hll.hpp"

#include <catch2/catch.hpp>
#include <test_allocator.hpp>

#include <algorithm>
#include <cstring>
#include <iterator>
#include <map>
#include <memory>
#include <sstream>
#include <type_traits>
#include <vector>

namespace datasketches {
namespace {

struct allocation_failure {
  long fail_at = -1;
  long calls = 0;
  bool failed = false;

  void arm(long allocation) {
    fail_at = allocation;
    calls = 0;
    failed = false;
  }

  void disarm() { fail_at = -1; }
};

template<typename T>
struct failing_allocator: test_allocator<T> {
  using value_type = T;
  template<typename U> struct rebind { using other = failing_allocator<U>; };

  explicit failing_allocator(allocation_failure& failure): test_allocator<T>(0), failure(&failure) {}
  template<typename U>
  failing_allocator(const failing_allocator<U>& other): test_allocator<T>(0), failure(other.failure) {}

  T* allocate(size_t n) {
    if (failure->fail_at >= 0 && failure->calls++ == failure->fail_at) {
      failure->failed = true;
      throw std::bad_alloc();
    }
    return test_allocator<T>::allocate(n);
  }

  allocation_failure* failure;
};

template<typename T, typename U>
bool operator==(const failing_allocator<T>&, const failing_allocator<U>&) { return true; }
template<typename T, typename U>
bool operator!=(const failing_allocator<T>&, const failing_allocator<U>&) { return false; }

using failure_sketch = hll_sketch_alloc<failing_allocator<uint8_t>>;
using failure_union = hll_union_alloc<failing_allocator<uint8_t>>;

template<typename T>
class allocator_pointer {
public:
  using element_type = T;
  using value_type = typename std::remove_cv<T>::type;
  using difference_type = std::ptrdiff_t;
  using reference = typename std::add_lvalue_reference<T>::type;
  using pointer = allocator_pointer;
  using iterator_category = std::random_access_iterator_tag;
  template<typename U> using rebind = allocator_pointer<U>;

  allocator_pointer() noexcept: ptr_(nullptr) {}
  allocator_pointer(std::nullptr_t) noexcept: ptr_(nullptr) {}
  allocator_pointer(T* ptr) noexcept: ptr_(ptr) {}
  template<typename U, typename std::enable_if<std::is_convertible<U*, T*>::value, int>::type = 0>
  allocator_pointer(const allocator_pointer<U>& other) noexcept: ptr_(other.get()) {}

  T* get() const noexcept { return ptr_; }
  T* operator->() const noexcept { return ptr_; }
  reference operator*() const { return *ptr_; }
  reference operator[](difference_type n) const { return ptr_[n]; }
  explicit operator bool() const noexcept { return ptr_ != nullptr; }
  operator typename std::conditional<std::is_const<T>::value, const void*, void*>::type() const noexcept {
    return ptr_;
  }
  allocator_pointer& operator++() { ++ptr_; return *this; }
  allocator_pointer operator++(int) { auto copy = *this; ++*this; return copy; }
  allocator_pointer& operator--() { --ptr_; return *this; }
  allocator_pointer operator--(int) { auto copy = *this; --*this; return copy; }
  allocator_pointer& operator+=(difference_type n) { ptr_ += n; return *this; }
  allocator_pointer& operator-=(difference_type n) { ptr_ -= n; return *this; }
  // Offsets match every integral type exactly. MSVC otherwise ranks its built-in arithmetic,
  // reached through the void* conversion, as equally good and reports an ambiguity.
  template<typename I>
  typename std::enable_if<std::is_integral<I>::value, allocator_pointer>::type operator+(I n) const { return ptr_ + n; }
  template<typename I>
  typename std::enable_if<std::is_integral<I>::value, allocator_pointer>::type operator-(I n) const { return ptr_ - n; }
  difference_type operator-(const allocator_pointer& other) const { return ptr_ - other.ptr_; }
  bool operator==(const allocator_pointer& other) const { return ptr_ == other.ptr_; }
  bool operator!=(const allocator_pointer& other) const { return ptr_ != other.ptr_; }
  bool operator==(std::nullptr_t) const noexcept { return ptr_ == nullptr; }
  bool operator!=(std::nullptr_t) const noexcept { return ptr_ != nullptr; }
  friend bool operator==(std::nullptr_t, const allocator_pointer& ptr) noexcept { return ptr.ptr_ == nullptr; }
  friend bool operator!=(std::nullptr_t, const allocator_pointer& ptr) noexcept { return ptr.ptr_ != nullptr; }
  bool operator<(const allocator_pointer& other) const { return ptr_ < other.ptr_; }
  bool operator>(const allocator_pointer& other) const { return ptr_ > other.ptr_; }
  bool operator<=(const allocator_pointer& other) const { return ptr_ <= other.ptr_; }
  bool operator>=(const allocator_pointer& other) const { return ptr_ >= other.ptr_; }
  template<typename I>
  friend typename std::enable_if<std::is_integral<I>::value, allocator_pointer>::type operator+(I n, allocator_pointer ptr) {
    return ptr + n;
  }

  template<typename U = T>
  static typename std::enable_if<!std::is_void<U>::value, allocator_pointer>::type pointer_to(U& value) noexcept {
    return std::addressof(value);
  }

private:
  T* ptr_;
};

template<typename T>
struct pointer_failing_allocator: failing_allocator<T> {
  using value_type = T;
  using pointer = allocator_pointer<T>;
  using const_pointer = allocator_pointer<const T>;
  template<typename U> struct rebind { using other = pointer_failing_allocator<U>; };

  explicit pointer_failing_allocator(allocation_failure& failure): failing_allocator<T>(failure) {}
  template<typename U>
  pointer_failing_allocator(const pointer_failing_allocator<U>& other): failing_allocator<T>(*other.failure) {}

  pointer allocate(size_t n) { return failing_allocator<T>::allocate(n); }
  void deallocate(pointer ptr, size_t n) { failing_allocator<T>::deallocate(ptr.get(), n); }
};

template<typename T, typename U>
bool operator==(const pointer_failing_allocator<T>&, const pointer_failing_allocator<U>&) { return true; }
template<typename T, typename U>
bool operator!=(const pointer_failing_allocator<T>&, const pointer_failing_allocator<U>&) { return false; }

struct allocator_pool {
  allocation_failure failure;
  const int id;
  long long allocations = 0;
  long long bytes = 0;

  explicit allocator_pool(int id): id(id) {}

  void check_no_leaks() const {
    REQUIRE(allocations == 0);
    REQUIRE(bytes == 0);
  }
};

template<typename T>
struct pool_failing_allocator: failing_allocator<T> {
  using value_type = T;
  template<typename U> struct rebind { using other = pool_failing_allocator<U>; };

  explicit pool_failing_allocator(allocator_pool& pool): failing_allocator<T>(pool.failure), pool(&pool) {}
  template<typename U>
  pool_failing_allocator(const pool_failing_allocator<U>& other):
    failing_allocator<T>(other.pool->failure), pool(other.pool) {}

  T* allocate(size_t n) {
    T* result = failing_allocator<T>::allocate(n);
    ++pool->allocations;
    pool->bytes += static_cast<long long>(n * sizeof(T));
    return result;
  }

  void deallocate(T* p, size_t n) {
    failing_allocator<T>::deallocate(p, n);
    --pool->allocations;
    pool->bytes -= static_cast<long long>(n * sizeof(T));
  }

  allocator_pool* pool;
};

template<typename T, typename U>
bool operator==(const pool_failing_allocator<T>& a, const pool_failing_allocator<U>& b) {
  return a.pool->id == b.pool->id;
}
template<typename T, typename U>
bool operator!=(const pool_failing_allocator<T>& a, const pool_failing_allocator<U>& b) { return !(a == b); }

using pool_failure_sketch = hll_sketch_alloc<pool_failing_allocator<uint8_t>>;
using pool_failure_union = hll_union_alloc<pool_failing_allocator<uint8_t>>;

struct selecting_allocator_state {
  struct allocation {
    unsigned pool;
    size_t bytes;
  };
  struct pool_balance {
    long long allocations = 0;
    long long bytes = 0;
  };

  allocation_failure failure;
  pool_balance pools[2];
  std::map<void*, allocation> live;
  unsigned mismatches = 0;

  void check_no_leaks() const {
    REQUIRE(mismatches == 0);
    REQUIRE(live.empty());
    for (const auto& pool: pools) {
      REQUIRE(pool.allocations == 0);
      REQUIRE(pool.bytes == 0);
    }
  }
};

template<typename T>
struct selecting_allocator: failing_allocator<T> {
  using value_type = T;
  template<typename U> struct rebind { using other = selecting_allocator<U>; };

  explicit selecting_allocator(selecting_allocator_state& state, unsigned pool = 0):
    failing_allocator<T>(state.failure), state(&state), pool(pool) {}
  template<typename U>
  selecting_allocator(const selecting_allocator<U>& other):
    failing_allocator<T>(other.state->failure), state(other.state), pool(other.pool) {}

  selecting_allocator select_on_container_copy_construction() const {
    return selecting_allocator(*state, 1 - pool);
  }

  T* allocate(size_t n) {
    T* result = failing_allocator<T>::allocate(n);
    state->live.emplace(result, selecting_allocator_state::allocation{pool, n * sizeof(T)});
    ++state->pools[pool].allocations;
    state->pools[pool].bytes += static_cast<long long>(n * sizeof(T));
    return result;
  }

  void deallocate(T* p, size_t n) noexcept {
    const auto it = state->live.find(p);
    if (it == state->live.end()) {
      ++state->mismatches;
      return;
    }
    if (it->second.pool != pool || it->second.bytes != n * sizeof(T)) ++state->mismatches;
    --state->pools[it->second.pool].allocations;
    state->pools[it->second.pool].bytes -= static_cast<long long>(it->second.bytes);
    state->live.erase(it);
    failing_allocator<T>::deallocate(p, n);
  }

  selecting_allocator_state* state;
  unsigned pool;
};

template<typename T, typename U>
bool operator==(const selecting_allocator<T>& a, const selecting_allocator<U>& b) {
  return a.state == b.state && a.pool == b.pool;
}
template<typename T, typename U>
bool operator!=(const selecting_allocator<T>& a, const selecting_allocator<U>& b) { return !(a == b); }

using selecting_sketch = hll_sketch_alloc<selecting_allocator<uint8_t>>;

struct allocation_balance {
  const long long allocations = test_allocator_net_allocations;
  const long long bytes = test_allocator_total_bytes;

  void check_no_leaks() const {
    REQUIRE(test_allocator_net_allocations == allocations);
    REQUIRE(test_allocator_total_bytes == bytes);
  }
};

template<typename A, typename B>
void check_equal(const hll_sketch_alloc<A>& actual, const hll_sketch_alloc<B>& expected) {
  const auto compact = actual.serialize_compact();
  const auto expected_compact = expected.serialize_compact();
  REQUIRE(compact.size() == expected_compact.size());
  REQUIRE(std::equal(compact.begin(), compact.end(), expected_compact.begin()));
  const auto updatable = actual.serialize_updatable();
  const auto expected_updatable = expected.serialize_updatable();
  REQUIRE(updatable.size() == expected_updatable.size());
  REQUIRE(std::equal(updatable.begin(), updatable.end(), expected_updatable.begin()));
  REQUIRE(actual.get_estimate() == expected.get_estimate());
  REQUIRE(actual.get_composite_estimate() == expected.get_composite_estimate());
}

template<typename A>
void check_equal(const hll_union_alloc<A>& actual, const hll_union_alloc<A>& expected) {
  check_equal(actual.get_result(HLL_8), expected.get_result(HLL_8));
  REQUIRE(actual.get_estimate() == expected.get_estimate());
}

template<typename Sketch>
void update_range(Sketch& sketch, uint64_t begin, uint64_t end) {
  for (uint64_t value = begin; value < end; ++value) sketch.update(value);
}

failure_sketch make_sketch(allocation_failure& failure, target_hll_type type, hll_mode mode, uint8_t lg_k = 12) {
  failure_sketch sketch(lg_k, type, false, failing_allocator<uint8_t>(failure));
  const uint32_t set_limit = 3 * (1U << (lg_k - 3)) / 4;
  const uint32_t count = mode == LIST ? 7 : (mode == SET ? std::min(100U, set_limit) : 1000);
  update_range(sketch, 0, count);
  if (mode == HLL) sketch.update(uint64_t(14441)); // register value 15, including an HLL_4 exception
  REQUIRE((sketch.serialize_compact()[hll_constants::MODE_BYTE] & 3) == mode);
  return sketch;
}

hll_sketch::vector_bytes maximum_set_image(uint8_t lg_k) {
  hll_sketch sketch(lg_k, HLL_8);
  const uint32_t capacity = uint32_t(1) << (lg_k - 3);
  const uint32_t limit = hll_constants::RESIZE_NUMER * capacity / hll_constants::RESIZE_DENOM;
  std::vector<bool> occupied(capacity, false);
  uint32_t count = 0;
  for (uint64_t value = 0; count < limit; ++value) {
    HashState hash;
    HllUtil<>::hash(&value, sizeof(value), DEFAULT_SEED, hash);
    const uint32_t slot = HllUtil<>::getLow26(HllUtil<>::coupon(hash)) & (capacity - 1);
    // Distinct final buckets keep the compact order independent of replay order.
    if (occupied[slot]) continue;
    occupied[slot] = true;
    sketch.update(value);
    ++count;
  }
  auto image = sketch.serialize_compact();
  REQUIRE((image[hll_constants::MODE_BYTE] & 3) == SET);
  uint32_t actual_count;
  std::memcpy(&actual_count, image.data() + hll_constants::HASH_SET_COUNT_INT, sizeof(actual_count));
  REQUIRE(actual_count == limit);
  return image;
}

template<typename Operation>
bool inject_failure(allocation_failure& failure, long allocation, Operation&& operation) {
  failure.arm(allocation);
  bool threw = false;
  try {
    operation();
  } catch (const std::bad_alloc&) {
    threw = true;
  }
  failure.disarm();
  REQUIRE(threw == failure.failed);
  return threw;
}

pool_failure_sketch make_pool_sketch(allocator_pool& pool, uint8_t lg_k, target_hll_type type, uint32_t count) {
  pool_failure_sketch sketch(lg_k, type, false, pool_failing_allocator<uint8_t>(pool));
  update_range(sketch, 0, count);
  REQUIRE((sketch.serialize_compact()[hll_constants::MODE_BYTE] & 3)
      == (count == 7 ? LIST : count == 1000 ? HLL : SET));
  return sketch;
}

long check_pool_union_update(uint8_t source_lg_k, target_hll_type type, uint32_t initial_count,
    uint32_t source_count, bool move, bool equal_allocators) {
  const allocation_balance balance;
  for (long allocation = 0;; ++allocation) {
    CAPTURE(source_lg_k, type, initial_count, source_count, move, equal_allocators, allocation);
    allocator_pool pool(0), input_pool(1), control_pool(2), control_input_pool(3);
    auto& source_pool = equal_allocators ? pool : input_pool;
    bool threw;
    {
      pool_failure_union sketch(12, pool_failing_allocator<uint8_t>(pool));
      pool_failure_union control(12, pool_failing_allocator<uint8_t>(control_pool));
      update_range(sketch, 10000, 10000 + initial_count);
      update_range(control, 10000, 10000 + initial_count);
      const auto before = sketch;
      const bool partial_merge = source_count != 1000
          && (before.get_result(HLL_8).serialize_compact()[hll_constants::MODE_BYTE] & 3) != HLL;
      {
        auto source = make_pool_sketch(equal_allocators ? control_pool : control_input_pool,
            source_lg_k, type, source_count);
        if (move) control.update(std::move(source));
        else control.update(source);
      }
      control_input_pool.check_no_leaks();
      {
        auto source = make_pool_sketch(source_pool, source_lg_k, type, source_count);
        const auto source_image = source.serialize_updatable();
        input_pool.failure.arm(0);
        threw = inject_failure(pool.failure, allocation, [&] {
          if (move) sketch.update(std::move(source));
          else sketch.update(source);
        });
        input_pool.failure.disarm();
        REQUIRE(input_pool.failure.calls == 0);
        REQUIRE_FALSE(input_pool.failure.failed);
        REQUIRE(pool.failure.calls == allocation + (threw ? 1 : 0));
        if (threw || !equal_allocators) REQUIRE(source.serialize_updatable() == source_image);
      }
      // The union must remain usable after the unequal input's entire pool is released.
      input_pool.check_no_leaks();
      if (threw) {
        if (!partial_merge) {
          check_equal(sketch, before);
        } else {
          auto partial = sketch.get_result(HLL_8);
          const auto image = partial.serialize_updatable();
          auto restored = pool_failure_sketch::deserialize(image.data(), image.size(),
              pool_failing_allocator<uint8_t>(pool));
          check_equal(partial, restored);
          auto probe = sketch;
          update_range(probe, 30000, 30200);
          update_range(partial, 30000, 30200);
          check_equal(probe.get_result(HLL_8), partial);
        }
        {
          auto source = make_pool_sketch(source_pool, source_lg_k, type, source_count);
          input_pool.failure.arm(0);
          if (move) sketch.update(std::move(source));
          else sketch.update(source);
          input_pool.failure.disarm();
          REQUIRE(input_pool.failure.calls == 0);
          REQUIRE_FALSE(input_pool.failure.failed);
        }
        input_pool.check_no_leaks();
      }
      check_equal(sketch, control);
      REQUIRE(sketch.get_lg_config_k() == control.get_lg_config_k());
      update_range(sketch, 20000, 20200);
      update_range(control, 20000, 20200);
      check_equal(sketch, control);
    }
    pool.check_no_leaks();
    input_pool.check_no_leaks();
    control_pool.check_no_leaks();
    control_input_pool.check_no_leaks();
    balance.check_no_leaks();
    if (!threw) return allocation;
  }
}

template<typename Factory>
void check_updates(Factory&& make, const std::vector<uint64_t>& values) {
  const allocation_balance balance;
  for (long allocation = 0;; ++allocation) {
    CAPTURE(allocation);
    allocation_failure failure, control_failure;
    {
      auto sketch = make(failure);
      auto control = make(control_failure);
      failure.arm(allocation);
      unsigned exceptions = 0;
      for (uint64_t value: values) {
        try {
          sketch.update(value);
        } catch (const std::bad_alloc&) {
          ++exceptions;
          failure.disarm();
          REQUIRE(failure.failed);
          check_equal(sketch, control);
          sketch.update(value);
        }
        control.update(value);
      }
      failure.disarm();
      REQUIRE(exceptions == (failure.failed ? 1 : 0));
      check_equal(sketch, control);
      update_range(sketch, 20000, 20200);
      update_range(control, 20000, 20200);
      check_equal(sketch, control);
    }
    balance.check_no_leaks();
    if (!failure.failed) break;
  }
}

uint64_t value_for_slot(uint32_t slot, uint8_t minimum, bool exact = false) {
  for (uint64_t value = 0;; ++value) {
    HashState hash;
    HllUtil<>::hash(&value, sizeof(value), DEFAULT_SEED, hash);
    const uint32_t coupon = HllUtil<>::coupon(hash);
    const uint8_t register_value = HllUtil<>::getValue(coupon);
    if ((HllUtil<>::getLow26(coupon) & 15) == slot
        && (exact ? register_value == minimum : register_value >= minimum)) return value;
  }
}

const std::vector<uint64_t>& selecting_shift_values() {
  static const auto values = [] {
    std::vector<uint64_t> result;
    for (uint32_t slot = 0; slot < 16; ++slot) {
      result.push_back(value_for_slot(slot, slot < 2 ? 18 : 3, true));
    }
    return result;
  }();
  return values;
}

const std::vector<uint64_t>& exception_values() {
  static const std::vector<uint64_t> values = {
    value_for_slot(0, 18), value_for_slot(1, 18), value_for_slot(2, 18),
    value_for_slot(3, 18), value_for_slot(4, 18)
  };
  return values;
}

enum class shift_error {
  empty_slots, missing_map, low_aux, orphan_aux, short_aux, missing_aux,
  dropped_aux, duplicate_aux, later_missing_map
};

void set_image_slot(hll_sketch::vector_bytes& image, uint32_t slot, uint8_t value) {
  auto& byte = image[hll_constants::HLL_BYTE_ARR_START + slot / 2];
  byte = static_cast<uint8_t>((slot & 1) ? (byte & 15) | (value << 4) : (byte & 240) | value);
}

hll_sketch::vector_bytes malformed_shift_image(shift_error error) {
  hll_sketch sketch(4, HLL_4, true);
  const bool no_map = error == shift_error::missing_map || error == shift_error::later_missing_map;
  for (uint32_t slot = 1; slot < 16; ++slot) {
    if (error == shift_error::empty_slots && slot == 8) continue;
    uint8_t value = static_cast<uint8_t>(error == shift_error::later_missing_map ? 2 : 1);
    if (slot == 1 && !no_map && error != shift_error::empty_slots) value = 18;
    sketch.update(value_for_slot(slot, value, true));
  }
  auto image = sketch.serialize_updatable();
  const uint32_t count = 1;
  std::memcpy(image.data() + hll_constants::CUR_MIN_COUNT_INT, &count, sizeof(count));
  if (no_map) set_image_slot(image, 1, hll_constants::AUX_TOKEN);
  if (error == shift_error::orphan_aux || error == shift_error::duplicate_aux) set_image_slot(image, 1, 1);
  if (error == shift_error::missing_aux || error == shift_error::dropped_aux) {
    set_image_slot(image, 2, hll_constants::AUX_TOKEN);
  }
  if (error == shift_error::low_aux || error == shift_error::short_aux
      || error == shift_error::dropped_aux || error == shift_error::duplicate_aux) {
    bool found = false;
    for (size_t offset = hll_constants::HLL_BYTE_ARR_START + 8; offset < image.size(); offset += sizeof(uint32_t)) {
      uint32_t coupon;
      std::memcpy(&coupon, image.data() + offset, sizeof(coupon));
      if (coupon != 0) {
        const uint8_t value = static_cast<uint8_t>(error == shift_error::low_aux ? 0 : error == shift_error::short_aux ? 2 : 15);
        coupon = HllUtil<>::pair(error == shift_error::duplicate_aux ? 0U : 1U, value);
        std::memcpy(image.data() + offset, &coupon, sizeof(coupon));
        found = true;
        break;
      }
    }
    REQUIRE(found);
  }
  return image;
}

void check_rejected_shift(failure_sketch& sketch, shift_error error, uint8_t new_value) {
  const auto item = value_for_slot(0, new_value, true);
  if (error == shift_error::empty_slots) {
    REQUIRE_THROWS_MATCHES(sketch.update(item), std::runtime_error,
        Catch::Matchers::Message("Array slots cannot be 0 at this point."));
  } else if (error == shift_error::duplicate_aux) {
    REQUIRE_THROWS_MATCHES(sketch.update(item), std::invalid_argument,
        Catch::Matchers::Message("Found a slotNo that should not be there: SlotNo: 0, Value: 15"));
  } else if (error == shift_error::dropped_aux || (error == shift_error::missing_map && new_value == 15)) {
    REQUIRE_THROWS_MATCHES(sketch.update(item), std::runtime_error,
        Catch::Matchers::Message("Inconsistent counts: auxCount: 0, HLL tokesn: 1"));
  } else if (error == shift_error::missing_map && new_value == 16) {
    REQUIRE_THROWS_MATCHES(sketch.update(item), std::runtime_error,
        Catch::Matchers::Message("Inconsistent counts: auxCount: 1, HLL tokesn: 2"));
  } else if (error == shift_error::missing_aux) {
    REQUIRE_THROWS_MATCHES(sketch.update(item), std::runtime_error,
        Catch::Matchers::Message("Inconsistent counts: auxCount: 1, HLL tokesn: 2"));
  } else {
    const char* message = error == shift_error::missing_map || error == shift_error::later_missing_map
        ? "auxHashMap cannot be null at this point"
        : error == shift_error::low_aux ? "oldActualVal < newCurMin when incrementing curMin"
        : error == shift_error::orphan_aux ? "getSlot(slotNum) != AUX_TOKEN for item in auxiliary hash map"
        : "newShiftedVal != 14 for item in old auxHashMap despite curMin increment";
    REQUIRE_THROWS_MATCHES(sketch.update(item), std::logic_error, Catch::Matchers::Message(message));
  }
}

} // namespace

TEST_CASE("hll allocation failure: construction", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool full_size = GENERATE(false, true);
  for (long allocation = 0;; ++allocation) {
    CAPTURE(type, full_size, allocation);
    allocation_failure failure;
    const bool threw = inject_failure(failure, allocation, [&] {
      failure_sketch sketch(12, type, full_size, failing_allocator<uint8_t>(failure));
      REQUIRE(sketch.is_empty());
    });
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll allocation failure: copy and conversion", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto source_type = GENERATE(HLL_4, HLL_6, HLL_8);
  const auto mode = GENERATE(LIST, SET, HLL);
  const bool convert = GENERATE(false, true);
  auto target_type = source_type;
  if (convert) {
    const auto converted_type = GENERATE(HLL_4, HLL_6, HLL_8);
    target_type = converted_type;
  }
  for (long allocation = 0;; ++allocation) {
    CAPTURE(source_type, target_type, mode, convert, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      auto source = make_sketch(failure, source_type, mode);
      auto control = make_sketch(control_failure, source_type, mode);
      failure_sketch expected(control, convert ? target_type : source_type);
      const auto copy = [&] {
        auto result = convert ? failure_sketch(source, target_type) : failure_sketch(source);
        failure.disarm();
        check_equal(result, expected);
        update_range(result, 20000, 20200);
        update_range(expected, 20000, 20200);
        check_equal(result, expected);
      };
      threw = inject_failure(failure, allocation, copy);
      check_equal(source, control);
      if (threw) copy();
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll allocation failure: copy assignment", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const auto source_mode = GENERATE(LIST, SET, HLL);
  const auto target_mode = GENERATE(LIST, SET, HLL);
  for (long allocation = 0;; ++allocation) {
    CAPTURE(type, source_mode, target_mode, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      auto source = make_sketch(failure, type, source_mode);
      auto source_control = make_sketch(control_failure, type, source_mode);
      auto sketch = make_sketch(failure, HLL_8, target_mode);
      auto control = make_sketch(control_failure, HLL_8, target_mode);
      threw = inject_failure(failure, allocation, [&] { sketch = source; });
      if (threw) {
        check_equal(sketch, control);
        sketch = source;
      }
      control = source_control;
      check_equal(sketch, control);
      check_equal(source, source_control);
      update_range(sketch, 20000, 20200);
      update_range(control, 20000, 20200);
      check_equal(sketch, control);
      const auto& same = sketch;
      sketch = same;
      check_equal(sketch, control);
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll allocation failure: reset", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const auto mode = GENERATE(LIST, SET, HLL);
  const bool full_size = GENERATE(false, true);
  for (long allocation = 0;; ++allocation) {
    CAPTURE(type, mode, full_size, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      auto sketch = make_sketch(failure, type, mode);
      auto control = make_sketch(control_failure, type, mode);
      threw = inject_failure(failure, allocation, [&] { sketch.reset(full_size); });
      if (threw) {
        check_equal(sketch, control);
        sketch.reset(full_size);
      }
      control.reset(full_size);
      check_equal(sketch, control);
      update_range(sketch, 20000, 21000);
      update_range(control, 20000, 21000);
      check_equal(sketch, control);
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll allocation failure: deserialization", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const auto mode = GENERATE(LIST, SET, HLL);
  const bool compact = GENERATE(false, true);
  const bool stream = GENERATE(false, true);
  for (long allocation = 0;; ++allocation) {
    CAPTURE(type, mode, compact, stream, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      auto control = make_sketch(control_failure, type, mode);
      const auto bytes = compact ? control.serialize_compact() : control.serialize_updatable();
      std::istringstream control_input(std::string(bytes.begin(), bytes.end()));
      auto expected = stream ? failure_sketch::deserialize(control_input, failing_allocator<uint8_t>(control_failure))
          : failure_sketch::deserialize(bytes.data(), bytes.size(), failing_allocator<uint8_t>(control_failure));
      const auto deserialize = [&] {
        std::istringstream input(std::string(bytes.begin(), bytes.end()));
        auto sketch = stream ? failure_sketch::deserialize(input, failing_allocator<uint8_t>(failure))
            : failure_sketch::deserialize(bytes.data(), bytes.size(), failing_allocator<uint8_t>(failure));
        failure.disarm();
        check_equal(sketch, expected);
        update_range(sketch, 20000, 20200);
        update_range(expected, 20000, 20200);
        check_equal(sketch, expected);
      };
      threw = inject_failure(failure, allocation, deserialize);
      if (threw) deserialize();
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll compact SET: maximum valid count", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const bool stream = GENERATE(false, true);
  for (uint8_t lg_k = 8; lg_k <= 21; ++lg_k) {
    CAPTURE(lg_k, stream);
    const auto image = maximum_set_image(lg_k);
    allocation_failure failure;
    {
      std::istringstream input(std::string(image.begin(), image.end()));
      auto sketch = stream ? failure_sketch::deserialize(input, failing_allocator<uint8_t>(failure))
          : failure_sketch::deserialize(image.data(), image.size(), failing_allocator<uint8_t>(failure));
      const auto restored = sketch.serialize_compact();
      REQUIRE(restored.size() == image.size());
      REQUIRE(std::equal(restored.begin(), restored.end(), image.begin()));
    }
    balance.check_no_leaks();
  }
}

TEST_CASE("hll compact SET: rejects overfull count before allocation", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const bool stream = GENERATE(false, true);
  const bool recompute_size = GENERATE(false, true);
  for (uint8_t lg_k = 8; lg_k <= 21; ++lg_k) {
    CAPTURE(lg_k, stream, recompute_size);
    auto image = maximum_set_image(lg_k);
    uint32_t count;
    std::memcpy(&count, image.data() + hll_constants::HASH_SET_COUNT_INT, sizeof(count));
    std::vector<uint32_t> coupons(count);
    std::memcpy(coupons.data(), image.data() + hll_constants::HASH_SET_INT_ARR_START, count * sizeof(uint32_t));
    uint32_t extra = HllUtil<>::pair(0, 63);
    while (std::find(coupons.begin(), coupons.end(), extra) != coupons.end()) ++extra;
    const size_t offset = image.size();
    image.resize(offset + sizeof(extra));
    std::memcpy(image.data() + offset, &extra, sizeof(extra));
    ++count;
    std::memcpy(image.data() + hll_constants::HASH_SET_COUNT_INT, &count, sizeof(count));
    if (recompute_size) image[hll_constants::LG_ARR_BYTE] = 0;
    std::istringstream input(std::string(image.begin(), image.end()));
    allocation_failure failure;
    failure.arm(0);
    if (stream) {
      REQUIRE_THROWS_AS(failure_sketch::deserialize(input, failing_allocator<uint8_t>(failure)), std::invalid_argument);
    } else {
      REQUIRE_THROWS_AS(failure_sketch::deserialize(image.data(), image.size(), failing_allocator<uint8_t>(failure)),
          std::invalid_argument);
    }
    failure.disarm();
    REQUIRE(failure.calls == 0);
    REQUIRE_FALSE(failure.failed);
    balance.check_no_leaks();
  }
}

TEST_CASE("hll compact SET: rejects invalid precision before allocation", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const bool stream = GENERATE(false, true);
  const uint8_t lg_k = GENERATE(uint8_t(22), uint8_t(255));
  auto image = maximum_set_image(8);
  image[hll_constants::LG_K_BYTE] = lg_k;
  image[hll_constants::LG_ARR_BYTE] = 0;
  std::istringstream input(std::string(image.begin(), image.end()));
  allocation_failure failure;
  failure.arm(0);
  if (stream) {
    REQUIRE_THROWS_AS(failure_sketch::deserialize(input, failing_allocator<uint8_t>(failure)), std::invalid_argument);
  } else {
    REQUIRE_THROWS_AS(failure_sketch::deserialize(image.data(), image.size(), failing_allocator<uint8_t>(failure)),
        std::invalid_argument);
  }
  failure.disarm();
  REQUIRE(failure.calls == 0);
  REQUIRE_FALSE(failure.failed);
  balance.check_no_leaks();
}

TEST_CASE("hll allocation failure: updates across modes", "[hll_allocation_failure]") {
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const uint8_t lg_k = GENERATE(uint8_t(4), uint8_t(7), uint8_t(12));
  const bool full_size = GENERATE(false, true);
  CAPTURE(type, lg_k, full_size);
  std::vector<uint64_t> values;
  for (uint64_t value = 0; value < 1000; ++value) values.push_back(value);
  values.push_back(14441);
  values.push_back(805951); // raises the same exception slot after a failed first auxiliary allocation
  check_updates([&](allocation_failure& failure) {
    return failure_sketch(lg_k, type, full_size, failing_allocator<uint8_t>(failure));
  }, values);
}

TEST_CASE("hll allocation failure: auxiliary map growth", "[hll_allocation_failure]") {
  const auto& values = exception_values();
  check_updates([&](allocation_failure& failure) {
    failure_sketch sketch(4, HLL_4, true, failing_allocator<uint8_t>(failure));
    for (size_t i = 1; i < 4; ++i) sketch.update(values[i]);
    return sketch;
  }, {values[4]});
}

TEST_CASE("hll allocation failure: auxiliary deserialization cleanup", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const bool stream = GENERATE(false, true);
  const uint32_t entries[4] = {0, 0, 0, 0};
  allocation_failure failure;
  const failing_allocator<uint8_t> allocator(failure);
  if (stream) {
    std::istringstream input(std::string(reinterpret_cast<const char*>(entries), sizeof(entries)));
    REQUIRE_THROWS_AS(AuxHashMap<failing_allocator<uint8_t>>::deserialize(input, 4, 1, 2, false, allocator),
        std::invalid_argument);
  } else {
    REQUIRE_THROWS_AS(AuxHashMap<failing_allocator<uint8_t>>::deserialize(entries, sizeof(entries), 4, 1, 2, false, allocator),
        std::invalid_argument);
  }
  balance.check_no_leaks();
}

TEST_CASE("hll allocation failure: promotions with auxiliary allocation", "[hll_allocation_failure]") {
  const bool set = GENERATE(false, true);
  const auto& values = exception_values();
  check_updates([&](allocation_failure& failure) {
    failure_sketch sketch(set ? 8 : 4, HLL_4, false, failing_allocator<uint8_t>(failure));
    for (size_t i = 1; i < values.size(); ++i) sketch.update(values[i]);
    update_range(sketch, 0, set ? 20 : 3);
    REQUIRE((sketch.serialize_compact()[hll_constants::MODE_BYTE] & 3) == (set ? SET : LIST));
    return sketch;
  }, {set ? uint64_t(20) : uint64_t(3)});
}

TEST_CASE("hll allocation failure: curMin shifts", "[hll_allocation_failure]") {
  const bool new_exception = GENERATE(false, true);
  const auto& values = exception_values();
  check_updates([&](allocation_failure& failure) {
    failure_sketch sketch(4, HLL_4, true, failing_allocator<uint8_t>(failure));
    for (uint32_t slot = 1; slot < 16; ++slot) {
      sketch.update(slot < values.size() ? values[slot] : value_for_slot(slot, 3, true));
    }
    return sketch;
  }, {new_exception ? values[0] : value_for_slot(0, 3, true)});
}

TEST_CASE("hll allocation failure: conversion with auxiliary growth", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto type = GENERATE(HLL_6, HLL_8);
  const bool shift = GENERATE(false, true);
  const auto& values = exception_values();
  for (long allocation = 0;; ++allocation) {
    CAPTURE(type, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      failure_sketch source(4, type, true, failing_allocator<uint8_t>(failure));
      failure_sketch control(4, type, true, failing_allocator<uint8_t>(control_failure));
      for (size_t i = 1; i < values.size(); ++i) {
        source.update(values[i]);
        control.update(values[i]);
      }
      if (shift) {
        for (uint32_t slot = 0; slot < 16; ++slot) {
          if (slot > 0 && slot < values.size()) continue;
          const auto item = value_for_slot(slot, 3, true);
          source.update(item);
          control.update(item);
        }
      }
      failure_sketch expected(control, HLL_4);
      const auto convert = [&] {
        failure_sketch sketch(source, HLL_4);
        failure.disarm();
        check_equal(sketch, expected);
        update_range(sketch, 20000, 20200);
        update_range(expected, 20000, 20200);
        check_equal(sketch, expected);
      };
      threw = inject_failure(failure, allocation, convert);
      check_equal(source, control);
      if (threw) convert();
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll selecting allocator: curMin shift", "[hll_allocation_failure][hll_selecting_allocator]") {
  const allocation_balance balance;
  selecting_allocator_state state;
  const auto& values = selecting_shift_values();
  {
    selecting_sketch sketch(4, HLL_4, true, selecting_allocator<uint8_t>(state));
    hll_sketch control(4, HLL_4, true);
    for (uint32_t slot = 1; slot < 16; ++slot) {
      sketch.update(values[slot]);
      control.update(values[slot]);
    }
    REQUIRE(sketch.serialize_updatable()[hll_constants::HLL_CUR_MIN_BYTE] == 0);
    sketch.update(values[0]);
    control.update(values[0]);
    check_equal(sketch, control);
    REQUIRE(sketch.serialize_updatable()[hll_constants::HLL_CUR_MIN_BYTE] == 3);
  }
  state.check_no_leaks();
  balance.check_no_leaks();
}

TEST_CASE("hll selecting allocator: conversion", "[hll_allocation_failure][hll_selecting_allocator]") {
  const allocation_balance balance;
  const auto type = GENERATE(HLL_6, HLL_8);
  selecting_allocator_state state;
  const auto& values = selecting_shift_values();
  const auto first = value_for_slot(0, 3, true);
  static const auto last = value_for_slot(15, 18, true);
  {
    selecting_sketch source(4, type, true, selecting_allocator<uint8_t>(state));
    hll_sketch control(4, type, true);
    for (uint32_t slot = 0; slot < 16; ++slot) {
      const auto item = slot == 0 ? first : slot == 15 ? last : values[slot];
      source.update(item);
      control.update(item);
    }
    const selecting_sketch converted(source, HLL_4);
    const hll_sketch expected(control, HLL_4);
    check_equal(converted, expected);
    check_equal(source, control);
  }
  state.check_no_leaks();
  balance.check_no_leaks();
}

TEST_CASE("hll selecting allocator: failed curMin shift", "[hll_allocation_failure][hll_selecting_allocator]") {
  const allocation_balance balance;
  const auto& values = selecting_shift_values();
  for (long allocation = 0;; ++allocation) {
    CAPTURE(allocation);
    selecting_allocator_state state;
    bool threw;
    {
      selecting_sketch sketch(4, HLL_4, true, selecting_allocator<uint8_t>(state));
      hll_sketch control(4, HLL_4, true);
      for (uint32_t slot = 1; slot < 16; ++slot) {
        sketch.update(values[slot]);
        control.update(values[slot]);
      }
      const auto before = sketch.serialize_updatable();
      threw = inject_failure(state.failure, allocation, [&] { sketch.update(values[0]); });
      REQUIRE(state.failure.calls == allocation + (threw ? 1 : 0));
      if (threw) {
        REQUIRE(sketch.serialize_updatable() == before);
        check_equal(sketch, control);
        sketch.update(values[0]);
      }
      control.update(values[0]);
      check_equal(sketch, control);
    }
    state.check_no_leaks();
    balance.check_no_leaks();
    if (!threw) {
      REQUIRE(allocation > 2);
      break;
    }
  }
}

TEST_CASE("hll allocation failure: union updates", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const auto mode = GENERATE(LIST, SET, HLL);
  const uint8_t lg_k = GENERATE(uint8_t(8), uint8_t(12), uint8_t(13));
  const uint32_t initial_count = GENERATE(0U, 7U, 24U, 100U, 1000U);
  const bool move = GENERATE(false, true);
  for (long allocation = 0;; ++allocation) {
    CAPTURE(type, mode, lg_k, initial_count, move, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      failure_union sketch(12, failing_allocator<uint8_t>(failure));
      failure_union control(12, failing_allocator<uint8_t>(control_failure));
      update_range(sketch, 10000, 10000 + initial_count);
      update_range(control, 10000, 10000 + initial_count);
      auto source = make_sketch(failure, type, mode, lg_k);
      auto source_control = make_sketch(control_failure, type, mode, lg_k);
      const bool partial_merge =
          (sketch.get_result(HLL_8).serialize_compact()[hll_constants::MODE_BYTE] & 3) != HLL && mode != HLL;
      const auto update = [&] {
        if (move) sketch.update(std::move(source));
        else sketch.update(source);
      };
      threw = inject_failure(failure, allocation, update);
      if (threw) {
        if (!partial_merge) check_equal(sketch, control);
        check_equal(source, source_control);
        update();
      }
      if (move) control.update(std::move(source_control));
      else control.update(source_control);
      check_equal(sketch, control);
      update_range(sketch, 20000, 20200);
      update_range(control, 20000, 20200);
      check_equal(sketch, control);
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll allocation failure: union reset and result", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const auto mode = GENERATE(LIST, SET, HLL);
  const bool reset = GENERATE(false, true);
  for (long allocation = 0;; ++allocation) {
    CAPTURE(type, mode, reset, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      failure_union sketch(12, failing_allocator<uint8_t>(failure));
      failure_union control(12, failing_allocator<uint8_t>(control_failure));
      auto source = make_sketch(failure, type, mode);
      auto source_control = make_sketch(control_failure, type, mode);
      sketch.update(source);
      control.update(source_control);
      check_equal(sketch, control);
      const auto operation = [&] {
        if (reset) sketch.reset();
        else {
          const auto result = sketch.get_result(type);
          failure.disarm();
          check_equal(result, control.get_result(type));
        }
      };
      threw = inject_failure(failure, allocation, operation);
      if (threw) {
        check_equal(sketch, control);
        operation();
      }
      if (reset) control.reset();
      check_equal(sketch, control);
      update_range(sketch, 20000, 20200);
      update_range(control, 20000, 20200);
      check_equal(sketch, control);
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll allocation failure: union copy assignment", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const auto mode = GENERATE(LIST, SET, HLL);
  for (long allocation = 0;; ++allocation) {
    CAPTURE(mode, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      failure_union source(8, failing_allocator<uint8_t>(failure));
      failure_union source_control(8, failing_allocator<uint8_t>(control_failure));
      auto input = make_sketch(failure, HLL_8, mode, 8);
      auto control_input = make_sketch(control_failure, HLL_8, mode, 8);
      source.update(input);
      source_control.update(control_input);
      failure_union sketch(12, failing_allocator<uint8_t>(failure));
      failure_union control(12, failing_allocator<uint8_t>(control_failure));
      update_range(sketch, 10000, 10007);
      update_range(control, 10000, 10007);
      threw = inject_failure(failure, allocation, [&] { sketch = source; });
      if (threw) {
        check_equal(sketch, control);
        auto probe = sketch;
        auto expected = control;
        auto dense = make_sketch(control_failure, HLL_8, HLL);
        probe.update(dense);
        expected.update(dense);
        check_equal(probe, expected);
        sketch = source;
      }
      control = source_control;
      check_equal(sketch, control);
      update_range(sketch, 20000, 20200);
      update_range(control, 20000, 20200);
      check_equal(sketch, control);
      const auto& same = sketch;
      sketch = same;
      check_equal(sketch, control);
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll allocation failure: sparse union merge retry", "[hll_allocation_failure]") {
  const allocation_balance balance;
  const uint32_t initial_count = GENERATE(6U, 23U, 95U, 375U);
  const uint32_t source_count = GENERATE(7U, 300U);
  const bool move = GENERATE(false, true);
  for (long allocation = 0;; ++allocation) {
    CAPTURE(initial_count, source_count, move, allocation);
    allocation_failure failure, control_failure;
    bool threw;
    {
      failure_union sketch(12, failing_allocator<uint8_t>(failure));
      failure_union control(12, failing_allocator<uint8_t>(control_failure));
      update_range(sketch, 10000, 10000 + initial_count);
      update_range(control, 10000, 10000 + initial_count);
      failure_sketch source(12, HLL_4, false, failing_allocator<uint8_t>(failure));
      update_range(source, 0, source_count);
      REQUIRE((source.serialize_compact()[hll_constants::MODE_BYTE] & 3) == (source_count == 7 ? LIST : SET));
      REQUIRE((sketch.get_result(HLL_8).serialize_compact()[hll_constants::MODE_BYTE] & 3) != HLL);
      control.update(source);
      const auto merge = [&] {
        if (move) sketch.update(std::move(source));
        else sketch.update(source);
      };
      threw = inject_failure(failure, allocation, merge);
      if (threw) {
        auto partial = sketch.get_result(HLL_8);
        const auto image = partial.serialize_updatable();
        auto restored = failure_sketch::deserialize(image.data(), image.size(), failing_allocator<uint8_t>(failure));
        check_equal(partial, restored);
        auto probe = sketch;
        update_range(probe, 30000, 30200);
        update_range(partial, 30000, 30200);
        check_equal(probe.get_result(HLL_8), partial);
        merge();
      }
      check_equal(sketch, control);
      update_range(sketch, 20000, 20200);
      update_range(control, 20000, 20200);
      check_equal(sketch, control);
    }
    balance.check_no_leaks();
    if (!threw) break;
  }
}

TEST_CASE("hll union allocators: LIST into empty union", "[hll_allocation_failure][hll_union_allocator]") {
  const uint8_t lg_k = GENERATE(uint8_t(8), uint8_t(12), uint8_t(13));
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  const long allocations = check_pool_union_update(lg_k, type, 0, 7, move, equal);
  if (!equal || lg_k != 12 || (move && type == HLL_8)) REQUIRE(allocations == 0);
  else REQUIRE(allocations == 2);
}

TEST_CASE("hll union allocators: SET into empty union", "[hll_allocation_failure][hll_union_allocator]") {
  const uint8_t lg_k = GENERATE(uint8_t(8), uint8_t(12), uint8_t(13));
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  const uint32_t count = lg_k == 8 ? 24U : lg_k == 12 ? 300U : 700U;
  const long allocations = check_pool_union_update(lg_k, type, 0, count, move, equal);
  if (equal && lg_k == 12 && move && type == HLL_8) REQUIRE(allocations == 0);
  else REQUIRE(allocations >= 2);
}

TEST_CASE("hll union allocators: sparse merge promotions", "[hll_allocation_failure][hll_union_allocator]") {
  const uint32_t initial_count = GENERATE(6U, 23U, 95U, 383U);
  const uint32_t source_count = GENERATE(7U, 300U, 700U);
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  const uint8_t lg_k = source_count == 700 ? uint8_t(13) : uint8_t(12);
  REQUIRE(check_pool_union_update(lg_k, type, initial_count, source_count, move, equal) > 0);
}

TEST_CASE("hll union allocators: HLL into empty union", "[hll_allocation_failure][hll_union_allocator]") {
  const uint8_t lg_k = GENERATE(uint8_t(8), uint8_t(12));
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  const long allocations = check_pool_union_update(lg_k, type, 0, 1000, move, equal);
  REQUIRE(allocations == (equal && move && type == HLL_8 ? 0 : 2));
}

TEST_CASE("hll union allocators: HLL source downsampling", "[hll_allocation_failure][hll_union_allocator]") {
  const uint32_t initial_count = GENERATE(0U, 7U, 100U);
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  REQUIRE(check_pool_union_update(13, type, initial_count, 1000, move, equal) == 2);
}

TEST_CASE("hll union allocators: HLL into sparse union", "[hll_allocation_failure][hll_union_allocator]") {
  const uint32_t initial_count = GENERATE(7U, 100U);
  const uint8_t lg_k = GENERATE(uint8_t(8), uint8_t(12));
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  REQUIRE(check_pool_union_update(lg_k, type, initial_count, 1000, move, equal) == 2);
}

TEST_CASE("hll union allocators: gadget downsampling", "[hll_allocation_failure][hll_union_allocator]") {
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  REQUIRE(check_pool_union_update(8, type, 1000, 1000, move, equal) == 2);
}

TEST_CASE("hll union allocators: sparse input into dense union", "[hll_allocation_failure][hll_union_allocator]") {
  const uint32_t source_count = GENERATE(7U, 300U, 700U);
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  const uint8_t lg_k = source_count == 700 ? uint8_t(13) : uint8_t(12);
  REQUIRE(check_pool_union_update(lg_k, type, 1000, source_count, move, equal) == 0);
}

TEST_CASE("hll union allocators: dense merge without downsampling", "[hll_allocation_failure][hll_union_allocator]") {
  const uint8_t lg_k = GENERATE(uint8_t(12), uint8_t(13));
  const auto type = GENERATE(HLL_4, HLL_6, HLL_8);
  const bool move = GENERATE(false, true);
  const bool equal = GENERATE(false, true);
  REQUIRE(check_pool_union_update(lg_k, type, 1000, 1000, move, equal) == 0);
}

TEST_CASE("hll malformed shift: preserves rejected image", "[hll_malformed]") {
  const allocation_balance balance;
  const auto error = GENERATE(shift_error::empty_slots, shift_error::missing_map, shift_error::low_aux,
      shift_error::orphan_aux, shift_error::short_aux, shift_error::missing_aux,
      shift_error::dropped_aux, shift_error::duplicate_aux, shift_error::later_missing_map);
  uint8_t new_value = static_cast<uint8_t>(
      error == shift_error::duplicate_aux || error == shift_error::later_missing_map ? 15 : 1);
  if (error == shift_error::empty_slots) {
    const auto value = GENERATE(1, 3, 15, 18);
    new_value = static_cast<uint8_t>(value);
  } else if (error == shift_error::missing_map) {
    const auto value = GENERATE(1, 15, 16);
    new_value = static_cast<uint8_t>(value);
  }
  const auto image = malformed_shift_image(error);
  allocation_failure failure;
  {
    auto sketch = failure_sketch::deserialize(image.data(), image.size(), failing_allocator<uint8_t>(failure));
    const auto before = sketch.serialize_updatable();
    const auto compact = sketch.serialize_compact();
    const double estimate = sketch.get_estimate();
    check_rejected_shift(sketch, error, new_value);
    REQUIRE(sketch.serialize_updatable() == before);
    REQUIRE(sketch.serialize_compact() == compact);
    REQUIRE(sketch.get_estimate() == estimate);
  }
  balance.check_no_leaks();
}

TEST_CASE("hll allocator pointer: nullable operations", "[hll_allocation_failure][hll_allocator_pointer]") {
  using pointer = allocator_pointer<uint8_t>;
  static_assert(std::is_same<decltype(std::declval<pointer>() == nullptr), bool>::value, "pointer == null");
  static_assert(std::is_same<decltype(nullptr == std::declval<pointer>()), bool>::value, "null == pointer");
  static_assert(std::is_same<decltype(std::declval<pointer>() != nullptr), bool>::value, "pointer != null");
  static_assert(std::is_same<decltype(nullptr != std::declval<pointer>()), bool>::value, "null != pointer");
  pointer value_initialized{};
  REQUIRE(value_initialized == nullptr);
  pointer p(nullptr);
  REQUIRE(p == nullptr);
  REQUIRE(nullptr == p);
  REQUIRE_FALSE(p != nullptr);
  REQUIRE_FALSE(nullptr != p);
  uint8_t value = 0;
  REQUIRE(std::pointer_traits<pointer>::pointer_to(value).get() == &value);
  p = pointer(&value);
  REQUIRE_FALSE(p == nullptr);
  REQUIRE_FALSE(nullptr == p);
  REQUIRE(p != nullptr);
  REQUIRE(nullptr != p);
  REQUIRE((p ? true : false));
  p = nullptr;
  REQUIRE_FALSE((p ? true : false));
  REQUIRE(p == nullptr);
}

TEST_CASE("hll allocator pointer: operations match raw pointers", "[hll_allocation_failure][hll_allocator_pointer]") {
  const allocation_balance balance;
  allocation_failure failure;
  using allocator = pointer_failing_allocator<uint8_t>;
  static_assert(!std::is_convertible<allocator::pointer, uint8_t*>::value, "allocator pointer must not convert to T*");
  using sketch = hll_sketch_alloc<allocator>;
  {
    sketch source(12, HLL_4, false, allocator(failure));
    hll_sketch expected_source(12, HLL_4);
    update_range(source, 0, 1000);
    update_range(expected_source, 0, 1000);
    source.update(uint64_t(14441));
    expected_source.update(uint64_t(14441));
    const auto source_image = source.serialize_compact();
    uint32_t aux_count;
    std::memcpy(&aux_count, source_image.data() + hll_constants::AUX_COUNT_INT, sizeof(aux_count));
    REQUIRE(aux_count > 0);
    const sketch copy(source);
    const hll_sketch expected_copy(expected_source);
    hll_union_alloc<allocator> result(12, allocator(failure));
    hll_union expected_result(12);
    result.update(copy);
    expected_result.update(expected_copy);
    const auto image = result.get_result(HLL_4).serialize_compact();
    const auto expected_image = expected_result.get_result(HLL_4).serialize_compact();
    REQUIRE(result.get_estimate() == expected_result.get_estimate());
    REQUIRE(image.size() == expected_image.size());
    REQUIRE(std::equal(image.begin(), image.end(), expected_image.begin()));
  }
  balance.check_no_leaks();
}

TEST_CASE("hll allocator pointer: construction failure releases storage", "[hll_allocation_failure][hll_allocator_pointer]") {
  const allocation_balance balance;
  allocation_failure failure;
  REQUIRE(inject_failure(failure, 1, [&] {
    hll_sketch_alloc<pointer_failing_allocator<uint8_t>> sketch(
        12, HLL_4, false, pointer_failing_allocator<uint8_t>(failure));
  }));
  REQUIRE(failure.calls == 2);
  balance.check_no_leaks();
}

} // namespace datasketches
