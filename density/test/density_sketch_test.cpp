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

#include <cmath>
#include <catch2/catch.hpp>

#include <density_sketch.hpp>
#include <test_allocator.hpp>

namespace datasketches {

TEST_CASE("density sketch: empty", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);
  REQUIRE(sketch.is_empty());
  REQUIRE_THROWS_AS(sketch.get_estimate({0, 0, 0}), std::runtime_error);
}

TEST_CASE("density sketch: one item", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);

  // dimension mismatch
  REQUIRE_THROWS_AS(sketch.update(std::vector<float>({0, 0})), std::invalid_argument);

  sketch.update(std::vector<float>({0, 0, 0}));
  REQUIRE_FALSE(sketch.is_empty());
  REQUIRE_FALSE(sketch.is_estimation_mode());
  REQUIRE(sketch.get_estimate({0, 0, 0}) == 1);
  REQUIRE(sketch.get_estimate({0.01, 0.01, 0.01}) > 0.95);
  REQUIRE(sketch.get_estimate({1, 1, 1}) < 0.05);
}

TEST_CASE("density sketch: merge", "[density_sketch]") {
  density_sketch<float> sketch1(10, 4);
  sketch1.update(std::vector<float>({0, 0, 0, 0}));
  sketch1.update(std::vector<float>({1, 2, 3, 4}));

  density_sketch<float> sketch2(10, 4);
  sketch2.update(std::vector<float>({5, 6, 7, 8}));

  sketch1.merge(sketch2);

  REQUIRE(sketch1.get_n() == 3);
  REQUIRE(sketch1.get_num_retained() == 3);
}

TEST_CASE("density sketch: iterator", "[density_sketch]") {
  density_sketch<float> sketch(10, 3);
  unsigned n = 1000;
  for (unsigned i = 1; i <= n; ++i) sketch.update(std::vector<float>(3, i));
  REQUIRE(sketch.get_n() == n);
  REQUIRE(sketch.is_estimation_mode());
  //std::cout << sketch.to_string(true, true);
  unsigned count = 0;
  for (auto pair: sketch) {
    ++count;
    // just to assert something about the output
    REQUIRE(pair.first.size() == sketch.get_dim());
  }
  REQUIRE(count == sketch.get_num_retained());
}

// spherical kernel for testing, returns 1 for vectors within radius and 0 otherwise
template<typename T>
struct spherical_kernel {
  spherical_kernel(T radius = 1.0) : _radius_squared(radius * radius) {}
  T operator()(const std::vector<T>& v1, const std::vector<T>& v2) const {
    return std::inner_product(v1.begin(), v1.end(), v2.begin(), 0.0, std::plus<T>(), [](T a, T b){return (a-b)*(a-b);}) <= _radius_squared ? 1.0 : 0.0;
  }
  private:
    T _radius_squared;
};

TEST_CASE("custom kernel", "[density_sketch]") {
  density_sketch<float, spherical_kernel<float>> sketch(10, 3, spherical_kernel<float>(0.5));

  // update with (1,1,1) and test points inside and outside the kernel
  sketch.update(std::vector<float>(3, 1.0));
  REQUIRE(sketch.get_estimate(std::vector<float>(3, 1.001)) == 1.0);
  REQUIRE(sketch.get_estimate(std::vector<float>(3, 2.0)) == 0.0);

  // rest of test follows iterator test above
  unsigned n = 1000;
  for (unsigned i = 2; i <= n; ++i) sketch.update(std::vector<float>(3, i));
  REQUIRE(sketch.get_n() == n);
  REQUIRE(sketch.is_estimation_mode());
  unsigned count = 0;
  for (auto pair: sketch) {
    ++count;
    // just to assert something about the output
    REQUIRE(pair.first.size() == sketch.get_dim());
  }
  REQUIRE(count == sketch.get_num_retained());
}

TEST_CASE("serialize empty", "[density_sketch]") {
  density_sketch<double> sk(10, 2);
  auto bytes = sk.serialize();
  REQUIRE(bytes.size() >= 3);
  REQUIRE(bytes[2] == 23);
  auto sk2 = density_sketch<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(sk2.is_empty());
  REQUIRE(!sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());

  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(s);
  const std::string stream_bytes = s.str();
  REQUIRE(stream_bytes.size() >= 3);
  REQUIRE(static_cast<uint8_t>(stream_bytes[2]) == 23);
  auto sk3 = density_sketch<double>::deserialize(s);
  REQUIRE(sk3.is_empty());
  REQUIRE(!sk3.is_estimation_mode());
  REQUIRE(sk.get_k() == sk3.get_k());
  REQUIRE(sk.get_dim() == sk3.get_dim());
  REQUIRE(sk.get_n() == sk3.get_n());
  REQUIRE(sk.get_num_retained() == sk3.get_num_retained());
}

TEST_CASE("serialize bytes", "[density_sketch]") {
  uint16_t k = 10;
  uint32_t dim = 3;
  density_sketch<double> sk(k, dim);

  for (uint16_t i = 0; i < k; ++i) {
    double val = static_cast<double>(i);
    sk.update(std::vector<double>({val, std::sqrt(val), -val}));
  }
  REQUIRE(!sk.is_estimation_mode());

  // exact mode
  auto bytes = sk.serialize();
  REQUIRE(bytes.size() >= 3);
  REQUIRE(bytes[2] == 23);
  auto sk2 = density_sketch<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(!sk2.is_empty());
  REQUIRE(!sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());
  auto it1 = sk.begin();
  auto it2 = sk2.begin();
  while (it1 != sk.end()) {
    REQUIRE(it1->first[0] == it2->first[0]);
    REQUIRE(it1->second == it2->second);
    ++it1;
    ++it2;
  }

  // estimation mode
  size_t n = 1031;
  for (uint32_t i = k; i < n; ++i) {
    double val = static_cast<double>(i);
    sk.update(std::vector<double>({val, std::sqrt(val), -val}));
  }
  REQUIRE(sk.is_estimation_mode());

  bytes = sk.serialize();
  REQUIRE(bytes.size() >= 3);
  REQUIRE(bytes[2] == 23);
  sk2 = density_sketch<double>::deserialize(bytes.data(), bytes.size());
  REQUIRE(!sk2.is_empty());
  REQUIRE(sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());
  it1 = sk.begin();
  it2 = sk2.begin();
  while (it1 != sk.end()) {
    REQUIRE(it1->first[0] == it2->first[0]);
    REQUIRE(it1->second == it2->second);
    ++it1;
    ++it2;
  }
}

TEST_CASE("serialize stream", "[density_sketch]") {
  uint16_t k = 10;
  uint32_t dim = 3;
  density_sketch<float> sk(k, dim);

  for (uint16_t i = 0; i < k; ++i) {
    float val = static_cast<float>(i);
    sk.update(std::vector<float>({val, std::sin(val), std::cos(val)}));
  }
  REQUIRE(!sk.is_estimation_mode());

  // exact mode
  std::stringstream s(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(s);
  REQUIRE(s.str().size() >= 3);
  REQUIRE(static_cast<uint8_t>(s.str()[2]) == 23);
  auto sk2 = density_sketch<float>::deserialize(s);
  REQUIRE(!sk2.is_empty());
  REQUIRE(!sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());
  auto it1 = sk.begin();
  auto it2 = sk2.begin();
  while (it1 != sk.end()) {
    REQUIRE(it1->first[0] == it2->first[0]);
    REQUIRE(it1->second == it2->second);
    ++it1;
    ++it2;
  }

  // estimation mode
  size_t n = 1031;
  for (uint32_t i = k; i < n; ++i) {
    float val = static_cast<float>(i);
    sk.update(std::vector<float>({val, std::sqrt(val), -val}));
  }
  REQUIRE(sk.is_estimation_mode());

  std::stringstream s2(std::ios::in | std::ios::out | std::ios::binary);
  sk.serialize(s2);
  REQUIRE(s2.str().size() >= 3);
  REQUIRE(static_cast<uint8_t>(s2.str()[2]) == 23);
  sk2 = density_sketch<float>::deserialize(s2);
  REQUIRE(!sk2.is_empty());
  REQUIRE(sk2.is_estimation_mode());
  REQUIRE(sk.get_k() == sk2.get_k());
  REQUIRE(sk.get_dim() == sk2.get_dim());
  REQUIRE(sk.get_n() == sk2.get_n());
  REQUIRE(sk.get_num_retained() == sk2.get_num_retained());
  it1 = sk.begin();
  it2 = sk2.begin();
  while (it1 != sk.end()) {
    REQUIRE(it1->first[0] == it2->first[0]);
    REQUIRE(it1->second == it2->second);
    ++it1;
    ++it2;
  }
}

TEST_CASE("density sketch: custom allocator", "[density_sketch]") {
  using alloc = test_allocator<double>;
  using sketch = density_sketch<double, gaussian_kernel<double>, alloc>;
  using point = std::vector<double, alloc>;
  test_allocator_total_bytes = 0;
  test_allocator_net_allocations = 0;
  {
    sketch s(10, 3, gaussian_kernel<double>(), alloc(0));
    // Keep every retained point close to the query so the Gaussian kernel cannot underflow to zero.
    for (int i = 0; i < 1000; ++i) s.update(point({i / 1000.0, 1.0, 2.0}, alloc(0)));
    REQUIRE(s.get_n() == 1000);
    REQUIRE(s.is_estimation_mode());
    REQUIRE(s.get_estimate({0.5, 1.0, 2.0}) > 0);
    REQUIRE(test_allocator_total_bytes > 0);

    sketch other(10, 3, gaussian_kernel<double>(), alloc(0));
    for (int i = 0; i < 100; ++i) other.update(point({i / 1000.0, 1.0, 2.0}, alloc(0)));
    s.merge(other);
    REQUIRE(s.get_n() == 1100);

    auto bytes = s.serialize();
    auto copy = sketch::deserialize(bytes.data(), bytes.size(), gaussian_kernel<double>(), alloc(0));
    REQUIRE(copy.get_n() == s.get_n());
    REQUIRE(copy.get_estimate({0.5, 1.0, 2.0}) == s.get_estimate({0.5, 1.0, 2.0}));
  }
  REQUIRE(test_allocator_total_bytes == 0);
  REQUIRE(test_allocator_net_allocations == 0);
}

TEST_CASE("density sketch: legacy family id 19 compatibility", "[density_sketch]") {
  // Legacy empty Density sketch (k = 10, dim = 2, family_id = 19)
  const std::vector<uint8_t> legacy_empty = {
    3, 1, 19, 4,
    10, 0, 0, 0,
    2, 0, 0, 0
  };
  {
    auto sk = density_sketch<double>::deserialize(legacy_empty.data(), legacy_empty.size());
    REQUIRE(sk.is_empty());
    REQUIRE(sk.get_k() == 10);
    REQUIRE(sk.get_dim() == 2);
    REQUIRE(sk.get_n() == 0);
    REQUIRE(sk.get_num_retained() == 0);

    auto upgraded = sk.serialize();
    REQUIRE(upgraded.size() == legacy_empty.size());
    REQUIRE(upgraded[2] == 23);

    std::stringstream is(
      std::string(reinterpret_cast<const char*>(legacy_empty.data()), legacy_empty.size()),
      std::ios::in | std::ios::out | std::ios::binary
    );
    auto sk_stream = density_sketch<double>::deserialize(is);
    REQUIRE(sk_stream.is_empty());
    REQUIRE(sk_stream.get_k() == 10);
    REQUIRE(sk_stream.get_dim() == 2);

    std::stringstream os(std::ios::in | std::ios::out | std::ios::binary);
    sk_stream.serialize(os);
    REQUIRE(static_cast<uint8_t>(os.str()[2]) == 23);
  }

  // Legacy single-point Density sketch (k = 10, dim = 2, point = [1.0, 2.0], family_id = 19)
  const std::vector<uint8_t> legacy_single = {
    6, 1, 19, 0,
    10, 0, 0, 0,
    2, 0, 0, 0,
    1, 0, 0, 0,
    1, 0, 0, 0, 0, 0, 0, 0,
    1, 0, 0, 0,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40
  };
  {
    auto sk = density_sketch<double>::deserialize(legacy_single.data(), legacy_single.size());
    REQUIRE(!sk.is_empty());
    REQUIRE(sk.get_k() == 10);
    REQUIRE(sk.get_dim() == 2);
    REQUIRE(sk.get_n() == 1);
    REQUIRE(sk.get_num_retained() == 1);
    REQUIRE(sk.get_estimate({1.0, 2.0}) == 1.0);

    auto upgraded = sk.serialize();
    REQUIRE(upgraded.size() == legacy_single.size());
    REQUIRE(upgraded[2] == 23);
    upgraded[2] = 19;
    REQUIRE(upgraded == legacy_single);

    std::stringstream is(
      std::string(reinterpret_cast<const char*>(legacy_single.data()), legacy_single.size()),
      std::ios::in | std::ios::out | std::ios::binary
    );
    auto sk_stream = density_sketch<double>::deserialize(is);
    REQUIRE(sk_stream.get_n() == 1);
    REQUIRE(sk_stream.get_estimate({1.0, 2.0}) == 1.0);
  }

  // Legacy estimation-mode Density sketch round-trip and re-serialization to family ID 23
  density_sketch<double> original(10, 3);
  for (uint32_t i = 0; i < 1031; ++i) {
    const double val = static_cast<double>(i);
    original.update(std::vector<double>({val, std::sqrt(val), -val}));
  }
  REQUIRE(original.is_estimation_mode());

  auto legacy_bytes = original.serialize();
  REQUIRE(legacy_bytes[2] == 23);
  legacy_bytes[2] = 19;

  auto from_bytes = density_sketch<double>::deserialize(legacy_bytes.data(), legacy_bytes.size());
  REQUIRE(from_bytes.is_estimation_mode());
  REQUIRE(from_bytes.get_k() == original.get_k());
  REQUIRE(from_bytes.get_dim() == original.get_dim());
  REQUIRE(from_bytes.get_n() == original.get_n());
  REQUIRE(from_bytes.get_num_retained() == original.get_num_retained());
  REQUIRE(from_bytes.get_estimate({5.0, std::sqrt(5.0), -5.0}) == original.get_estimate({5.0, std::sqrt(5.0), -5.0}));
  REQUIRE(from_bytes.serialize()[2] == 23);

  std::stringstream legacy_stream(
    std::string(reinterpret_cast<const char*>(legacy_bytes.data()), legacy_bytes.size()),
    std::ios::in | std::ios::out | std::ios::binary
  );
  auto from_stream = density_sketch<double>::deserialize(legacy_stream);
  REQUIRE(from_stream.get_n() == original.get_n());
  REQUIRE(from_stream.get_num_retained() == original.get_num_retained());
  REQUIRE(from_stream.get_estimate({5.0, std::sqrt(5.0), -5.0}) == original.get_estimate({5.0, std::sqrt(5.0), -5.0}));
  std::stringstream out_stream(std::ios::in | std::ios::out | std::ios::binary);
  from_stream.serialize(out_stream);
  REQUIRE(static_cast<uint8_t>(out_stream.str()[2]) == 23);
}

TEST_CASE("density sketch: reject invalid family and ebpps headers", "[density_sketch]") {
  // Unrelated family ID (20 = T-Digest)
  const std::vector<uint8_t> wrong_family = {
    3, 1, 20, 4,
    10, 0, 0, 0,
    2, 0, 0, 0
  };
  REQUIRE_THROWS_AS(
    density_sketch<double>::deserialize(wrong_family.data(), wrong_family.size()),
    std::invalid_argument
  );
  {
    std::stringstream is(
      std::string(reinterpret_cast<const char*>(wrong_family.data()), wrong_family.size()),
      std::ios::in | std::ios::out | std::ios::binary
    );
    REQUIRE_THROWS_AS(density_sketch<double>::deserialize(is), std::invalid_argument);
  }

  // Empty EBPPS header (preamble_longs = 1, ser_ver = 1, family_id = 19, flags = 4, 8 bytes)
  const std::vector<uint8_t> ebpps_empty = {
    1, 1, 19, 4,
    10, 0, 0, 0
  };
  REQUIRE_THROWS_AS(
    density_sketch<double>::deserialize(ebpps_empty.data(), ebpps_empty.size()),
    std::out_of_range
  );
  {
    std::stringstream is(
      std::string(reinterpret_cast<const char*>(ebpps_empty.data()), ebpps_empty.size()),
      std::ios::in | std::ios::out | std::ios::binary
    );
    REQUIRE_THROWS_AS(density_sketch<double>::deserialize(is), std::runtime_error);
  }

  // Padded empty EBPPS header (preamble_longs = 1, family_id = 19, flags = 4, 12 bytes)
  const std::vector<uint8_t> ebpps_empty_padded = {
    1, 1, 19, 4,
    10, 0, 0, 0,
    2, 0, 0, 0
  };
  REQUIRE_THROWS_AS(
    density_sketch<double>::deserialize(ebpps_empty_padded.data(), ebpps_empty_padded.size()),
    std::invalid_argument
  );
  {
    std::stringstream is(
      std::string(reinterpret_cast<const char*>(ebpps_empty_padded.data()), ebpps_empty_padded.size()),
      std::ios::in | std::ios::out | std::ios::binary
    );
    REQUIRE_THROWS_AS(density_sketch<double>::deserialize(is), std::invalid_argument);
  }

  // Non-empty EBPPS header (preamble_longs = 5, ser_ver = 1, family_id = 19, flags = 0 / 8)
  for (uint8_t flags : {uint8_t{0}, uint8_t{8}}) {
    std::vector<uint8_t> ebpps_non_empty(40, 0);
    ebpps_non_empty[0] = 5;
    ebpps_non_empty[1] = 1;
    ebpps_non_empty[2] = 19;
    ebpps_non_empty[3] = flags;
    ebpps_non_empty[4] = 10;
    REQUIRE_THROWS_AS(
      density_sketch<double>::deserialize(ebpps_non_empty.data(), ebpps_non_empty.size()),
      std::invalid_argument
    );
    std::stringstream is(
      std::string(reinterpret_cast<const char*>(ebpps_non_empty.data()), ebpps_non_empty.size()),
      std::ios::in | std::ios::out | std::ios::binary
    );
    REQUIRE_THROWS_AS(density_sketch<double>::deserialize(is), std::invalid_argument);
  }

  // Unexpected flag bits (bit 3 = 8) must be rejected for both legacy (19) and current (23) IDs
  for (uint8_t fam : {uint8_t{19}, uint8_t{23}}) {
    std::vector<uint8_t> bad_flags(24, 0);
    bad_flags[0] = 6;
    bad_flags[1] = 1;
    bad_flags[2] = fam;
    bad_flags[3] = 8;
    bad_flags[4] = 10;
    bad_flags[8] = 2;
    REQUIRE_THROWS_AS(
      density_sketch<double>::deserialize(bad_flags.data(), bad_flags.size()),
      std::invalid_argument
    );
    std::stringstream is(
      std::string(reinterpret_cast<const char*>(bad_flags.data()), bad_flags.size()),
      std::ios::in | std::ios::out | std::ios::binary
    );
    REQUIRE_THROWS_AS(density_sketch<double>::deserialize(is), std::invalid_argument);
  }
}

} /* namespace datasketches */
