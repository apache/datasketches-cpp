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
#include <sstream>
#include <stdexcept>

#include "hll.hpp"

namespace datasketches {

static void println(std::string& str) {
  unused(str);
  //std::cout << str << "\n";
}

static void basicUnion(uint64_t n1, uint64_t n2,
		                   uint8_t lgk1, uint8_t lgk2, uint8_t lgMaxK,
                       target_hll_type type1, target_hll_type type2, target_hll_type resultType) {
  uint64_t v = 0;
  //int tot = n1 + n2;

  hll_sketch h1(lgk1, type1);
  hll_sketch h2(lgk2, type2);
  uint8_t lgControlK = std::min(std::min(lgk1, lgk2), lgMaxK);
  hll_sketch control(lgControlK, resultType);

  for (uint64_t i = 0; i < n1; ++i) {
    h1.update(v + i);
    control.update(v + i);
  }
  v += n1;
  for (uint64_t i = 0; i < n2; ++i) {
    h2.update(v + i);
    control.update(v + i);
  }
  v += n2;

  hll_union u(lgMaxK);
  u.update(h1);
  u.update(h2);

  hll_sketch result = u.get_result(resultType);

  // ensure we check a direct union estimate, without first calling get_result()
  u.reset();
  u.update(std::move(h1));
  u.update(h2);

  // force non-HIP estimates to avoid issues with in- vs out-of-order
  double uEst = result.get_composite_estimate();
  double uUb = result.get_upper_bound(2);
  double uLb = result.get_lower_bound(2);
  //double rerr = ((uEst/tot) - 1.0) * 100;

  double controlEst = control.get_composite_estimate();
  double controlUb = control.get_upper_bound(2);
  double controlLb = control.get_lower_bound(2);

  REQUIRE((controlUb - controlEst) >= 0.0);
  REQUIRE((uUb - uEst) >= 0.0);
  REQUIRE((controlEst - controlLb) >= 0.0);
  REQUIRE((uEst - uLb) >= 0.0);

  REQUIRE(controlEst == uEst);
  REQUIRE(controlEst == u.get_composite_estimate());
}

/**
 * The task here is to check the transition boundaries as the sketch morphs between LIST to
 * SET to HLL modes. The transition points vary as a function of lgConfigK. In addition,
 * this checks that the union operation is operating properly based on the order the
 * sketches are presented to the union.
 */
TEST_CASE("hll union: check unions", "[hll_union]") {
  target_hll_type type1 = HLL_8;
  target_hll_type type2 = HLL_8;
  target_hll_type resultType = HLL_8;

  uint8_t lgK1 = 7;
  uint8_t lgK2 = 7;
  uint8_t lgMaxK = 7;
  uint64_t n1 = 7;
  uint64_t n2 = 7;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
  n1 = 8;
  n2 = 7;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
  n1 = 7;
  n2 = 8;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
  n1 = 8;
  n2 = 8;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
  n1 = 7;
  n2 = 14;
  basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);

  uint8_t i = 0;
  for (i = 7; i <= 13; ++i) {
    lgK1 = i;
    lgK2 = i;
    lgMaxK = i;
    {
      n1 = ((1 << (i - 3)) * 3)/4; // compute the transition point
      n2 = n1;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 -= 2;
      n2 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    }
    lgK1 = i;
    lgK2 = i + 1;
    lgMaxK = i;
    {
      n1 = ((1 << (i - 3)) * 3)/4;
      n2 = n1;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 -= 2;
      n2 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    }
    lgK1 = i + 1;
    lgK2 = i;
    lgMaxK = i;
    {
      n1 = ((1 << (i - 3)) * 3)/4;
      n2 = n1;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 -= 2;
      n2 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    }
    lgK1 = i + 1;
    lgK2 = i + 1;
    lgMaxK = i;
    {
      n1 = ((1 << (i - 3)) * 3)/4;
      n2 = n1;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 -= 2;
      n2 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
      n1 += 2;
      basicUnion(n1, n2, lgK1, lgK2, lgMaxK, type1, type2, resultType);
    }
  }
}

TEST_CASE("hll union: check composite estimate", "[hll_union]") {
  hll_union u(12);
  REQUIRE(u.is_empty());
  REQUIRE(u.get_composite_estimate() == Approx(0.0).margin(0.03));
  for (int i = 1; i <= 15; ++i) { u.update(i); }
  REQUIRE(u.get_composite_estimate() == Approx(15.0).margin(15 * 0.03));
  for (int i = 16; i <= 1000; ++i) { u.update(i); }
  REQUIRE(u.get_composite_estimate() == Approx(1000.0).margin(1000 * 0.03));
}

TEST_CASE("hll union: check config k limits", "[hll_union]") {
  REQUIRE_THROWS_AS(hll_union(hll_constants::MIN_LOG_K - 1), std::invalid_argument);

  REQUIRE_THROWS_AS(hll_union(hll_constants::MAX_LOG_K + 1), std::invalid_argument);
}

static double getBound(int lgK, bool ub, bool oooFlag, int numStdDev, double est) {
  double re = RelativeErrorTables<>::getRelErr(ub, oooFlag, lgK, numStdDev);
  return est / (1.0 + re);
}

TEST_CASE("hll union: check ub lb", "[hll_union]") {
  uint8_t lgK = 4;
  int n = 1 << 20;
  bool oooFlag = false;
  
  double bound;
  std::string str;

  bound = (getBound(lgK, true, oooFlag, 3, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", UB3: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, true, oooFlag, 2, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", UB2: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, true, oooFlag, 1, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", UB1: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, false, oooFlag, 1, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", LB1: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, false, oooFlag, 2, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", LB2: " + std::to_string(bound);
  println(str);
  bound = (getBound(lgK, false, oooFlag, 3, n) / n) - 1;
  str = "LgK=" + std::to_string(lgK) + ", LB3: " + std::to_string(bound);
  println(str);
}

TEST_CASE("hll union: check conversions", "[hll_union]") {
  uint8_t lgK = 4;
  hll_sketch sk1(lgK, HLL_8);
  hll_sketch sk2(lgK, HLL_8);
  int n = 1 << 20;
  for (int i = 0; i < n; ++i) {
    sk1.update(i);
    sk2.update(i + n);
  }
  hll_union hllUnion(lgK);
  hllUnion.update(sk1);
  hllUnion.update(sk2);

  hll_sketch rsk1 = hllUnion.get_result(HLL_4);
  hll_sketch rsk2 = hllUnion.get_result(HLL_6);
  hll_sketch rsk3 = hllUnion.get_result(HLL_8);
  double est1 = rsk1.get_estimate();
  double est2 = rsk2.get_estimate();
  double est3 = rsk3.get_estimate();
  REQUIRE(est1 == est2);
  REQUIRE(est1 == est3);
}

TEST_CASE("hll union: check input types", "[hll_union]") {
  hll_union u(8);

  // inserting the same value as a variety of input types
  u.update((uint8_t) 102);
  u.update((uint16_t) 102);
  u.update((uint32_t) 102);
  u.update((uint64_t) 102);
  u.update((int8_t) 102);
  u.update((int16_t) 102);
  u.update((int32_t) 102);
  u.update((int64_t) 102);
  REQUIRE(u.get_estimate() == Approx(1.0).margin(0.01));

  // identical binary representations
  // no unsigned in Java, but need to sign-extend both as Java would do
  u.update((uint8_t) 255);
  u.update((int8_t) -1);

  u.update((float) -2.0);
  u.update((double) -2.0);

  std::string str = "input string";
  u.update(str);
  u.update(str.c_str(), str.length());
  REQUIRE(u.get_estimate() == Approx(4.0).margin(0.01));

  u = hll_union(8);
  u.update((float) 0.0);
  u.update((float) -0.0);
  u.update((double) 0.0);
  u.update((double) -0.0);
  REQUIRE(u.get_estimate() == Approx(1.0).margin(0.01));

  u = hll_union(8);
  u.update(std::nanf("3"));
  u.update(std::nan("12"));
  REQUIRE(1.0 == Approx(u.get_estimate()).margin(0.01));
  REQUIRE(u.get_result().get_estimate() == Approx(u.get_estimate()).margin(0.01));

  u = hll_union(8);
  u.update(nullptr, 0);
  u.update("");
  REQUIRE(u.is_empty());
}

static void union_two_sketches_with_overlap(int num, uint8_t lg_k, target_hll_type type) {
  hll_sketch sketch1(lg_k, type);
  for (int key = 0; key < num; key++) sketch1.update(key);

  const int overlap = num / 2;
  hll_sketch sketch2(lg_k, type);
  for (int key = overlap; key < num + overlap; key++) sketch2.update(key);

  hll_union u(lg_k);
  u.update(sketch1);
  u.update(sketch2);
  hll_sketch sketch = u.get_result(type);
  REQUIRE(sketch.get_estimate() == Approx(num * 1.5).margin(num * 1.5 * 0.02));
}

TEST_CASE("hll union: check hll to hll", "[hll_union]") {
  union_two_sketches_with_overlap(1000000, 11, HLL_4);
}

static hll_sketch make_hll_sketch(uint8_t lg_config_k, uint64_t start, uint64_t end) {
  hll_sketch sketch(lg_config_k);
  for (uint64_t i = start; i < end; ++i) sketch.update(i);
  return sketch;
}

static hll_sketch::vector_bytes serialize_flat_union(
    const hll_sketch& first, const hll_sketch& second, const hll_sketch& third) {
  hll_union u(8);
  u.update(first);
  u.update(second);
  u.update(third);
  return u.get_result(HLL_8).serialize_updatable();
}

static hll_sketch::vector_bytes serialize_nested_union(
    const hll_sketch& first, const hll_sketch& second, const hll_sketch& third) {
  hll_union prefix(8);
  prefix.update(first);
  prefix.update(second);

  hll_union u(8);
  u.update(prefix.get_result(HLL_8));
  u.update(third);
  return u.get_result(HLL_8).serialize_updatable();
}

TEST_CASE("hll union: downsampling merge is not empty", "[hll_union]") {
  const hll_sketch sketch = make_hll_sketch(15, 0, 100000);
  hll_union u(8);
  u.update(sketch);
  REQUIRE_FALSE(u.is_empty());
}

TEST_CASE("hll union: mixed lgConfigK estimate is merge-order independent", "[hll_union]") {
  const uint64_t n = 100000;
  const hll_sketch a = make_hll_sketch(15, 0, n);
  const hll_sketch b = make_hll_sketch(8, n, 2 * n);
  const double truth = 2.0 * n;

  hll_union larger_first(8);
  larger_first.update(a);
  larger_first.update(b);
  REQUIRE(larger_first.get_estimate() == Approx(truth).epsilon(0.1));

  hll_union smaller_first(8);
  smaller_first.update(b);
  smaller_first.update(a);
  REQUIRE(smaller_first.get_estimate() == Approx(truth).epsilon(0.1));
}

TEST_CASE("hll union: scalar update after downsampling merge", "[hll_union]") {
  const uint64_t n = 100000;
  const hll_sketch sketch = make_hll_sketch(15, 0, n);
  hll_union u(8);
  u.update(sketch);
  for (uint64_t i = n; i < 2 * n; ++i) u.update(i);
  REQUIRE(u.get_estimate() == Approx(2.0 * n).epsilon(0.1));
}

TEST_CASE("hll union: serialization is grouping independent", "[hll_union]") {
  const uint64_t n = 100000;
  const hll_sketch a = make_hll_sketch(15, 0, n);
  const hll_sketch b = make_hll_sketch(8, n, 2 * n);
  const hll_sketch c = make_hll_sketch(11, n / 2, n + n / 2);
  REQUIRE(serialize_nested_union(b, c, a) == serialize_flat_union(b, c, a));
  REQUIRE(serialize_nested_union(a, c, b) == serialize_flat_union(a, c, b));
  REQUIRE(serialize_nested_union(a, b, c) == serialize_flat_union(a, b, c));
}

// A stateful allocator: instances with different ids are different allocators, and the bytes each one
// has outstanding are counted.
static long long pool_bytes[2] = {0, 0};

template<typename T>
struct pool_allocator {
  using value_type = T;
  int id;
  explicit pool_allocator(int id): id(id) {}
  template<typename U>
  pool_allocator(const pool_allocator<U>& other): id(other.id) {}
  T* allocate(std::size_t n) {
    pool_bytes[id] += n * sizeof(T);
    return static_cast<T*>(::operator new(n * sizeof(T)));
  }
  void deallocate(T* p, std::size_t n) {
    pool_bytes[id] -= n * sizeof(T);
    ::operator delete(p);
  }
};
template<typename T, typename U>
bool operator==(const pool_allocator<T>& a, const pool_allocator<U>& b) { return a.id == b.id; }
template<typename T, typename U>
bool operator!=(const pool_allocator<T>& a, const pool_allocator<U>& b) { return a.id != b.id; }

using pool_hll_sketch = hll_sketch_alloc<pool_allocator<uint8_t>>;
using pool_hll_union = hll_union_alloc<pool_allocator<uint8_t>>;

static pool_hll_sketch make_pool_sketch(int id, uint8_t lg_k, target_hll_type type, uint64_t n) {
  pool_hll_sketch sketch(lg_k, type, false, pool_allocator<uint8_t>(id));
  for (uint64_t i = 0; i < n; ++i) sketch.update(i);
  return sketch;
}

TEST_CASE("hll union: keeps no memory of an input sketch's allocator", "[hll_union]") {
  const uint8_t lg_max_k = 10;
  // LIST, SET and HLL inputs, with a smaller, the same and a larger lg_k than the union
  const uint64_t sizes[] = {5, 100, 10000};
  const uint8_t lg_ks[] = {8, 10, 12};
  for (uint64_t n: sizes) {
    for (uint8_t lg_k: lg_ks) {
      for (target_hll_type type: {HLL_4, HLL_6, HLL_8}) {
        for (bool gadget_empty: {true, false}) {
          for (bool move: {false, true}) {
            hll_union u(lg_max_k);
            pool_hll_union pool_union(lg_max_k, pool_allocator<uint8_t>(0));
            if (!gadget_empty) {
              for (uint64_t i = 0; i < 3 * n; ++i) {
                u.update(i + n);
                pool_union.update(i + n);
              }
            }
            {
              hll_sketch control(lg_k, type);
              for (uint64_t i = 0; i < n; ++i) control.update(i);
              u.update(control);
              pool_hll_sketch input = make_pool_sketch(1, lg_k, type, n);
              if (move) {
                pool_union.update(std::move(input));
              } else {
                pool_union.update(input);
              }
            }
            REQUIRE(pool_bytes[1] == 0);
            REQUIRE(pool_union.get_estimate() == u.get_estimate());
          }
        }
      }
    }
  }
  REQUIRE(pool_bytes[0] == 0);
}

} /* namespace datasketches */
