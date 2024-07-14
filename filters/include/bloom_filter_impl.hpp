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

#ifndef _BLOOM_FILTER_IMPL_HPP_
#define _BLOOM_FILTER_IMPL_HPP_

#include <memory>
#include <vector>

#include "common_defs.hpp"
#include "xxhash64.h"

namespace datasketches {

template<typename A>
bloom_filter_alloc<A>::bloom_filter_alloc(const uint64_t num_bits, const uint16_t num_hashes, uint64_t seed, const A& allocator) :
  allocator_(allocator),
  seed_(seed),
  num_hashes_(num_hashes),
  bit_array_(num_bits, allocator)
  {}

template<typename A>
bool bloom_filter_alloc<A>::is_empty() const {
  return bit_array_.is_empty();
}

template<typename A>
uint64_t bloom_filter_alloc<A>::get_bits_used() const {
  return bit_array_.get_num_bits_set();
}

template<typename A>
uint64_t bloom_filter_alloc<A>::get_capacity() const {
  return bit_array_.get_capacity();
}

template<typename A>
uint16_t bloom_filter_alloc<A>::get_num_hashes() const {
  return num_hashes_;
}

template<typename A>
uint64_t bloom_filter_alloc<A>::get_seed() const {
  return seed_;
}

template<typename A>
void bloom_filter_alloc<A>::reset() {
  bit_array_.reset();
}

// UPDATE METHODS

template<typename A>
void bloom_filter_alloc<A>::update(const std::string& item) {
  if (item.empty()) return;
  const uint64_t h0 = XXHash64::hash(item.data(), item.size(), seed_);
  const uint64_t h1 = XXHash64::hash(item.data(), item.size(), h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::update(const uint64_t item) {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::update(const uint32_t item) {
  update(static_cast<uint64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(const uint16_t item) {
  update(static_cast<uint64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(const uint8_t item) {
  update(static_cast<uint64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(const int64_t item) {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::update(const int32_t item) {
  update(static_cast<int64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(const int16_t item) {
  update(static_cast<int64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(const int8_t item) {
  update(static_cast<int64_t>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(const double item) {
  union {
    int64_t long_value;
    double double_value;
  } ldu;
  ldu.doubleBytes = static_cast<double>(item);
  if (item == 0.0) {
    ldu.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(ldu.doubleBytes)) {
    ldu.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  const uint64_t h0 = XXHash64::hash(&ldu, sizeof(ldu), seed_);
  const uint64_t h1 = XXHash64::hash(&ldu, sizeof(ldu), h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::update(const float item) {
  update(static_cast<double>(item));
}

template<typename A>
void bloom_filter_alloc<A>::update(const void* item, size_t size) {
  if (item == nullptr || size == 0) return;
  const uint64_t h0 = XXHash64::hash(item, size, seed_);
  const uint64_t h1 = XXHash64::hash(item, size, h0);
  internal_update(h0, h1);
}

template<typename A>
void bloom_filter_alloc<A>::internal_update(const uint64_t h0, const uint64_t h1) {
  const uint64_t num_bits = bit_array_.get_capacity();
  for (uint16_t i = 1; i <= num_hashes_; i++) {
    const uint64_t hash_index = (h0 + i * h1; >> 1) % num_bits;
    bit_array_.set_bit(bit_index);
  }
}

// QUERY-AND-UPDATE METHODS

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const std::string& item) {
  if (item.empty()) return;
  const uint64_t h0 = XXHash64::hash(item.data(), item.size(), seed_);
  const uint64_t h1 = XXHash64::hash(item.data(), item.size(), h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const uint64_t item) {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const uint32_t item) {
  return query_and_update(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const uint16_t item) {
  return query_and_update(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const uint8_t item) {
  return query_and_update(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const int64_t item) {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const int32_t item) {
  return query_and_update(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const int16_t item) {
  return query_and_update(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const int8_t item) {
  return query_and_update(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const double item) {
  union {
    int64_t long_value;
    double double_value;
  } ldu;
  ldu.doubleBytes = item;
  if (item == 0.0) {
    ldu.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(ldu.doubleBytes)) {
    ldu.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  const uint64_t h0 = XXHash64::hash(&ldu, sizeof(ldu), seed_);
  const uint64_t h1 = XXHash64::hash(&ldu, sizeof(ldu), h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const float item) {
  return query_and_update(static_cast<double>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const void* item, size_t size) {
  if (item == nullptr || size == 0) return;
  const uint64_t h0 = XXHash64::hash(item, size, seed_);
  const uint64_t h1 = XXHash64::hash(item, size, h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::internal_query_and_update(const uint64_t h0, const uint64_t h1) {
  const uint64_t num_bits = bit_array_.get_capacity();
  bool value_exists = true;
  for (uint16_t i = 0; i < num_hashes_; i++) {
    const uint64_t hash_index = (h0 + i * h1; >> 1) % num_bits;
    value_exists &= bit_array_.get_and_set_bit(bit_index);
  }
  return value_exists;
}

// QUERY METHODS

template<typename A>
bool bloom_filter_alloc<A>::query(const std::string& item) const {
  if (item.empty()) return;
  const uint64_t h0 = XXHash64::hash(item.data(), item.size(), seed_);
  const uint64_t h1 = XXHash64::hash(item.data(), item.size(), h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query(const uint64_t item) const {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query(const uint32_t item) const {
  return query(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(const uint16_t item) const {
  return query(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(const uint8_t item) const {
  return query(static_cast<uint64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(const int64_t item) const {
  const uint64_t h0 = XXHash64::hash(&item, sizeof(item), seed_);
  const uint64_t h1 = XXHash64::hash(&item, sizeof(item), h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query(const int32_t item) const {
  return query(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(const int16_t item) const {
  return query(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(const int8_t item) const {
  return query(static_cast<int64_t>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(const double item) const {
  union {
    int64_t long_value;
    double double_value;
  } ldu;
  ldu.doubleBytes = static_cast<double>(item);
  if (item == 0.0) {
    ldu.doubleBytes = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(ldu.doubleBytes)) {
    ldu.longBytes = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
  }
  const uint64_t h0 = XXHash64::hash(&ldu, sizeof(ldu), seed_);
  const uint64_t h1 = XXHash64::hash(&ldu, sizeof(ldu), h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::query(const float item) const {
  return query(static_cast<double>(item));
}

template<typename A>
bool bloom_filter_alloc<A>::query(const void* item, size_t size) const {
  if (item == nullptr || size == 0) return;
  const uint64_t h0 = XXHash64::hash(item, size, seed_);
  const uint64_t h1 = XXHash64::hash(item, size, h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::internal_query(const uint64_t h0, const uint64_t h1) const {
  const uint64_t num_bits = bit_array_.get_capacity();
  for (uint16_t i = 0; i < num_hashes_; i++) {
    const uint64_t hash_index = (h0 + i * h1; >> 1) % num_bits;
    if (!bit_array_.get_bit(bit_index))
      return false;
  }
  return true;
}

// OTHER METHODS

template<typename A>
bool bloom_filter_alloc<A>::is_compatible(const bloom_filter_alloc& other) const {
  return seed_ == other.seed_ && num_hashes_ == other.num_hashes_ && bit_array_.get_capacity() == other.bit_array_.get_capacity();
}

template<typename A>
void bloom_filter_alloc<A>::union_with(const bloom_filter_alloc& other) {
  if (!is_compatible(other)) {
    throw std::invalid_argument("Incompatible bloom filters");
  }
  bit_array_.union_with(other.bit_array_);
}

template<typename A>
void bloom_filter_alloc<A>::intersect(const bloom_filter_alloc& other) {
  if (!is_compatible(other)) {
    throw std::invalid_argument("Incompatible bloom filters");
  }
  bit_array_.intersect(other.bit_array_);
}

template<typename A>
void bloom_filter_alloc<A>::invert() {
  bit_array_.invert();
}

} // namespace datasketches

#endif // _BLOOM_FILTER_IMPL_HPP_