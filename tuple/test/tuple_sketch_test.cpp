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

#include <iostream>
#include <tuple>
#include <stdexcept>

namespace datasketches {

using three_doubles = std::tuple<double, double, double>;

// this is needed for a test below, but should be defined here
std::ostream& operator<<(std::ostream& os, const three_doubles& tuple) {
  os << std::get<0>(tuple) << ", " << std::get<1>(tuple) << ", " << std::get<2>(tuple);
  return os;
}

}

#include <catch2/catch.hpp>
#include <tuple_sketch.hpp>

namespace datasketches {

TEST_CASE("tuple sketch float: builder", "[tuple_sketch]") {
  auto builder = update_tuple_sketch<float>::builder();
  builder.set_lg_k(10).set_p(0.5f).set_resize_factor(theta_constants::resize_factor::X2).set_seed(123);
  auto sketch = builder.build();
  REQUIRE(sketch.get_lg_k() == 10);
  REQUIRE(sketch.get_theta() == 1.0); // empty sketch should have theta 1.0
  REQUIRE(sketch.get_rf() == theta_constants::resize_factor::X2);
  REQUIRE(sketch.get_seed_hash() == compute_seed_hash(123));
  sketch.update(1, 0);
  REQUIRE(sketch.get_theta() == 0.5); // theta = p
}

TEST_CASE("tuple sketch: min lg_k", "[tuple_sketch]") {
  // Tuple sketches reuse theta's builder and hash table, so the same nominal floor
  // (MIN_LG_K = 4, matching Java's ThetaUtil.MIN_LG_NOM_LONGS) and cache floor (MIN_LG_ARR = 5)
  // apply. lg_k = 4 (nominal 16) is the smallest allowed nominal size; below it must throw.
  REQUIRE(theta_constants::MIN_LG_K == 4);
  REQUIRE_THROWS_AS(update_tuple_sketch<float>::builder().set_lg_k(theta_constants::MIN_LG_K - 1),
      std::invalid_argument);
  auto min_sketch = update_tuple_sketch<float>::builder().set_lg_k(theta_constants::MIN_LG_K).build();
  REQUIRE(min_sketch.get_lg_k() == theta_constants::MIN_LG_K);

  // update well past the nominal size to force estimation mode and exercise the rebuild path,
  // tracking the peak number of retained entries seen between rebuilds.
  const int n = 10000;
  uint32_t max_retained = 0;
  for (int i = 0; i < n; ++i) {
    min_sketch.update(i, 1.0f);
    if (min_sketch.get_num_retained() > max_retained) max_retained = min_sketch.get_num_retained();
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
  auto deserialized = compact_tuple_sketch<float>::deserialize(bytes.data(), bytes.size());
  REQUIRE(deserialized.get_num_retained() == min_sketch.get_num_retained());
  REQUIRE(deserialized.get_estimate() == min_sketch.get_estimate());
}

TEST_CASE("tuple sketch float: empty", "[tuple_sketch]") {
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  std::cout << "sizeof(update_tuple_sketch<float>)=" << sizeof(update_sketch) << std::endl;
  REQUIRE(update_sketch.is_empty());
  REQUIRE(!update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_estimate() == 0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0);
  REQUIRE(update_sketch.get_lower_bound(1, 1) == 0);
  REQUIRE(update_sketch.get_lower_bound(1, update_sketch.get_num_retained()) == 0);
  REQUIRE(update_sketch.get_lower_bound(1, update_sketch.get_num_retained()+1) == 0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0);
  REQUIRE(update_sketch.get_upper_bound(1, 1) == 0);
  REQUIRE(update_sketch.get_upper_bound(1, update_sketch.get_num_retained()) == 0);
  REQUIRE(update_sketch.get_upper_bound(1, update_sketch.get_num_retained()+1) == 0);
  REQUIRE(update_sketch.get_theta() == 1);
  REQUIRE(update_sketch.get_num_retained() == 0);
  REQUIRE(update_sketch.is_ordered());

  auto compact_sketch = update_sketch.compact();
  std::cout << "sizeof(compact_tuple_sketch<float>)=" << sizeof(compact_sketch) << std::endl;
  REQUIRE(compact_sketch.is_empty());
  REQUIRE(!compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_estimate() == 0);
  REQUIRE(compact_sketch.get_lower_bound(1) == 0);
  REQUIRE(compact_sketch.get_lower_bound(1, 1) == 0);
  REQUIRE(compact_sketch.get_lower_bound(1, update_sketch.get_num_retained()) == 0);
  REQUIRE(compact_sketch.get_upper_bound(1) == 0);
  REQUIRE(compact_sketch.get_upper_bound(1, 1) == 0);
  REQUIRE(compact_sketch.get_upper_bound(1, update_sketch.get_num_retained()) == 0);
  REQUIRE(compact_sketch.get_theta() == 1);
  REQUIRE(compact_sketch.get_num_retained() == 0);
  REQUIRE(compact_sketch.is_ordered());

  // empty is forced to be ordered
  REQUIRE(update_sketch.compact(false).is_ordered());
}

TEST_CASE("tuple sketch: single item", "[tuple_sketch]") {
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  update_sketch.update(1, 1.0f);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_theta() == 1.0);
  REQUIRE(update_sketch.get_estimate() == 1.0);
  REQUIRE(update_sketch.get_lower_bound(1) == 1.0);
  REQUIRE(update_sketch.get_upper_bound(1) == 1.0);
  REQUIRE(update_sketch.is_ordered()); // one item is ordered

  auto compact_sketch = update_sketch.compact();
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

TEST_CASE("tuple sketch float: exact mode", "[tuple_sketch]") {
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  update_sketch.update(1, 1.0f);
  update_sketch.update(2, 2.0f);
  update_sketch.update(1, 1.0f);
//  std::cout << update_sketch.to_string(true);
  REQUIRE_FALSE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_estimate() == 2);
  REQUIRE(update_sketch.get_lower_bound(1) == 2);
  REQUIRE(update_sketch.get_lower_bound(1, 1) == 1);
  REQUIRE(update_sketch.get_lower_bound(1, update_sketch.get_num_retained()) == 2);
  REQUIRE(update_sketch.get_upper_bound(1) == 2);
  REQUIRE(update_sketch.get_upper_bound(1, 1) == 1);
  REQUIRE(update_sketch.get_upper_bound(1, update_sketch.get_num_retained()) == 2);
  REQUIRE(update_sketch.get_theta() == 1);
  REQUIRE(update_sketch.get_num_retained() == 2);
  REQUIRE_FALSE(update_sketch.is_ordered());
  int count = 0;
  for (const auto& entry: update_sketch) {
    REQUIRE(entry.second == 2);
    ++count;
  }
  REQUIRE(count == 2);

  auto compact_sketch = update_sketch.compact();
//  std::cout << compact_sketch.to_string(true);
  REQUIRE_FALSE(compact_sketch.is_empty());
  REQUIRE_FALSE(compact_sketch.is_estimation_mode());
  REQUIRE(compact_sketch.get_estimate() == 2);
  REQUIRE(compact_sketch.get_lower_bound(1) == 2);
  REQUIRE(compact_sketch.get_lower_bound(1, 1) == 1);
  REQUIRE(compact_sketch.get_lower_bound(1, compact_sketch.get_num_retained()) == 2);
  REQUIRE(compact_sketch.get_upper_bound(1) == 2);
  REQUIRE(compact_sketch.get_upper_bound(1, 1) == 1);
  REQUIRE(compact_sketch.get_upper_bound(1, compact_sketch.get_num_retained()) == 2);
  REQUIRE(compact_sketch.get_theta() == 1);
  REQUIRE(compact_sketch.get_num_retained() == 2);
  REQUIRE(compact_sketch.is_ordered());
  count = 0;
  for (const auto& entry: compact_sketch) {
    REQUIRE(entry.second == 2);
    ++count;
  }
  REQUIRE(count == 2);

  { // stream
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    compact_sketch.serialize(s);
    auto deserialized_sketch = compact_tuple_sketch<float>::deserialize(s);
    REQUIRE(!deserialized_sketch.is_empty());
    REQUIRE(!deserialized_sketch.is_estimation_mode());
    REQUIRE(deserialized_sketch.get_estimate() == 2);
    REQUIRE(deserialized_sketch.get_lower_bound(1) == 2);
    REQUIRE(deserialized_sketch.get_lower_bound(1, 1) == 1);
    REQUIRE(deserialized_sketch.get_lower_bound(1, deserialized_sketch.get_num_retained()) == 2);
    REQUIRE(deserialized_sketch.get_upper_bound(1) == 2);
    REQUIRE(deserialized_sketch.get_upper_bound(1, 1) == 1);
    REQUIRE(deserialized_sketch.get_upper_bound(1, deserialized_sketch.get_num_retained()) == 2);
    REQUIRE(deserialized_sketch.get_theta() == 1);
    REQUIRE(deserialized_sketch.get_num_retained() == 2);
    REQUIRE(deserialized_sketch.is_ordered());
//    std::cout << "deserialized sketch:" << std::endl;
//    std::cout << deserialized_sketch.to_string(true);
  }
  { // bytes
    auto bytes = compact_sketch.serialize();
    auto deserialized_sketch = compact_tuple_sketch<float>::deserialize(bytes.data(), bytes.size());
    REQUIRE(!deserialized_sketch.is_empty());
    REQUIRE(!deserialized_sketch.is_estimation_mode());
    REQUIRE(deserialized_sketch.get_estimate() == 2);
    REQUIRE(deserialized_sketch.get_lower_bound(1) == 2);
    REQUIRE(deserialized_sketch.get_lower_bound(1, 1) == 1);
    REQUIRE(deserialized_sketch.get_lower_bound(1, deserialized_sketch.get_num_retained()) == 2);
    REQUIRE(deserialized_sketch.get_upper_bound(1) == 2);
    REQUIRE(deserialized_sketch.get_upper_bound(1, 1) == 1);
    REQUIRE(deserialized_sketch.get_upper_bound(1, deserialized_sketch.get_num_retained()) == 2);
    REQUIRE(deserialized_sketch.get_theta() == 1);
    REQUIRE(deserialized_sketch.get_num_retained() == 2);
    REQUIRE(deserialized_sketch.is_ordered());
//    std::cout << deserialized_sketch.to_string(true);
  }
  // mixed
  {
    auto bytes = compact_sketch.serialize();
    std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
    s.write(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    auto deserialized_sketch = compact_tuple_sketch<float>::deserialize(s);
    auto it = deserialized_sketch.begin();
    for (const auto& entry: compact_sketch) {
      REQUIRE(entry.first == (*it).first);
      REQUIRE(entry.second == (*it).second);
      ++it;
    }
  }

  update_sketch.reset();
  REQUIRE(update_sketch.is_empty());
  REQUIRE_FALSE(update_sketch.is_estimation_mode());
  REQUIRE(update_sketch.get_estimate() == 0);
  REQUIRE(update_sketch.get_lower_bound(1) == 0);
  REQUIRE(update_sketch.get_upper_bound(1) == 0);
  REQUIRE(update_sketch.get_theta() == 1);
  REQUIRE(update_sketch.get_num_retained() == 0);
  REQUIRE(update_sketch.is_ordered());
}

template<typename T>
class max_value_policy {
public:
  max_value_policy(const T& initial_value): initial_value(initial_value) {}
  T create() const { return initial_value; }
  void update(T& summary, const T& update) const { summary = std::max(summary, update); }
private:
  T initial_value;
};

using max_float_update_tuple_sketch = update_tuple_sketch<float, float, max_value_policy<float>>;

TEST_CASE("tuple sketch: float, custom policy", "[tuple_sketch]") {
  auto update_sketch = max_float_update_tuple_sketch::builder(max_value_policy<float>(5)).build();
  update_sketch.update(1, 1.0f);
  update_sketch.update(1, 2.0f);
  update_sketch.update(2, 10.0f);
  update_sketch.update(3, 3.0f);
  update_sketch.update(3, 7.0f);
//  std::cout << update_sketch.to_string(true);
  int count = 0;
  float sum = 0;
  for (const auto& entry: update_sketch) {
    sum += entry.second;
    ++count;
  }
  REQUIRE(count == 3);
  REQUIRE(sum == 22); // 5 + 10 + 7
}

struct three_doubles_update_policy {
  std::tuple<double, double, double> create() const {
    return std::tuple<double, double, double>(0, 0, 0);
  }
  void update(std::tuple<double, double, double>& summary, const std::tuple<double, double, double>& update) const {
    std::get<0>(summary) += std::get<0>(update);
    std::get<1>(summary) += std::get<1>(update);
    std::get<2>(summary) += std::get<2>(update);
  }
};

TEST_CASE("tuple sketch: tuple of doubles", "[tuple_sketch]") {
  using three_doubles_update_tuple_sketch = update_tuple_sketch<three_doubles, three_doubles, three_doubles_update_policy>;
  auto update_sketch = three_doubles_update_tuple_sketch::builder().build();
  update_sketch.update(1, three_doubles(1, 2, 3));
//  std::cout << update_sketch.to_string(true);
  const auto& entry = *update_sketch.begin();
  REQUIRE(std::get<0>(entry.second) == 1.0);
  REQUIRE(std::get<1>(entry.second) == 2.0);
  REQUIRE(std::get<2>(entry.second) == 3.0);

  auto compact_sketch = update_sketch.compact();
//  std::cout << compact_sketch.to_string(true);
  REQUIRE(compact_sketch.get_num_retained() == 1);
}

TEST_CASE("tuple sketch: float, update with different types of keys", "[tuple_sketch]") {
  auto sketch = update_tuple_sketch<float>::builder().build();

  sketch.update(static_cast<uint64_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<int64_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<uint32_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<int32_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<uint16_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<int16_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<uint8_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(static_cast<int8_t>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 1);

  sketch.update(1.0, 1.0f);
  REQUIRE(sketch.get_num_retained() == 2);

  sketch.update(static_cast<float>(1), 1.0f);
  REQUIRE(sketch.get_num_retained() == 2);

  sketch.update("a", 1.0f);
  REQUIRE(sketch.get_num_retained() == 3);
}

TEST_CASE("filter", "[tuple_sketch]") {
  auto usk = update_tuple_sketch<int>::builder().build();

  { // empty update sketch
    auto sk = usk.filter([](int){return true;});
    REQUIRE(sk.is_empty());
    REQUIRE(sk.is_ordered());
    REQUIRE(sk.get_num_retained() == 0);
  }

  { // empty compact sketch
    auto sk = usk.compact().filter([](int){return true;});
    REQUIRE(sk.is_empty());
    REQUIRE(sk.is_ordered());
    REQUIRE(sk.get_num_retained() == 0);
  }

  usk.update(1, 1);
  usk.update(1, 1);
  usk.update(2, 1);
  usk.update(2, 1);
  usk.update(3, 1);

  { // exact mode update sketch
    auto sk = usk.filter([](int v){return v > 1;});
    REQUIRE_FALSE(sk.is_empty());
    REQUIRE_FALSE(sk.is_ordered());
    REQUIRE_FALSE(sk.is_estimation_mode());
    REQUIRE(sk.get_num_retained() == 2);
  }

  { // exact mode compact sketch
    auto sk = usk.compact().filter([](int v){return v > 1;});
    REQUIRE_FALSE(sk.is_empty());
    REQUIRE(sk.is_ordered());
    REQUIRE_FALSE(sk.is_estimation_mode());
    REQUIRE(sk.get_num_retained() == 2);
  }

  // only keys 1 and 2 had values of 2, which will become 3 after this update
  // some entries are discarded in estimation mode, but these happen to survive
  // the process is deterministic, so the test will always work
  for (int i = 0; i < 10000; ++i) usk.update(i, 1);

  { // estimation mode update sketch
    auto sk = usk.filter([](int v){return v > 2;});
    REQUIRE_FALSE(sk.is_empty());
    REQUIRE_FALSE(sk.is_ordered());
    REQUIRE(sk.is_estimation_mode());
    REQUIRE(sk.get_num_retained() == 2);
  }

  { // estimation mode compact sketch
    auto sk = usk.compact().filter([](int v){return v > 2;});
    REQUIRE_FALSE(sk.is_empty());
    REQUIRE(sk.is_ordered());
    REQUIRE(sk.is_estimation_mode());
    REQUIRE(sk.get_num_retained() == 2);
  }
}

TEST_CASE("tuple sketch: deserialize bounds-checks each entry key", "[tuple_sketch]") {
  // A compact sketch serialized with a narrower summary (float, 4 bytes) and then
  // deserialized as a wider summary (double, 8 bytes). The per-entry stride the reader
  // assumes (8-byte key + 8-byte summary) is larger than the entries actually occupy
  // (8-byte key + 4-byte summary), so the read cursor advances past the end of the buffer.
  // num_entries is read from the preamble and is unaffected, so the entry loop still runs
  // the full count and the per-entry key read walks off the end. This must throw rather
  // than read out of bounds (a heap-buffer-overflow under AddressSanitizer).
  auto update_sketch = update_tuple_sketch<float>::builder().build();
  for (int i = 0; i < 100; ++i) update_sketch.update(i, 1.0f);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_tuple_sketch<double>::deserialize(bytes.data(), bytes.size()),
                    std::out_of_range);
}

TEST_CASE("tuple sketch: deserialize rejects unconsumed bytes", "[tuple_sketch]") {
  // A compact sketch serialized with a wider summary (double, 8 bytes) and then
  // deserialized as a narrower summary (float, 4 bytes). The reader consumes less
  // data than the entries occupy, so deserialization must fail if bytes remain 
  // after the last entry.
  auto update_sketch = update_tuple_sketch<double>::builder().build();
  update_sketch.update(1, 1.0);
  auto bytes = update_sketch.compact().serialize();
  REQUIRE_THROWS_AS(compact_tuple_sketch<float>::deserialize(bytes.data(), bytes.size()),
                    std::out_of_range);
}

} /* namespace datasketches */
