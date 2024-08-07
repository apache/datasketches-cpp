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
#include "bit_array_ops.hpp"
#include "xxhash64.h"

namespace datasketches {

template<typename A>
bloom_filter_alloc<A>::bloom_filter_alloc(const uint64_t num_bits, const uint16_t num_hashes, uint64_t seed, const A& allocator) :
  allocator_(allocator),
  seed_(seed),
  num_hashes_(num_hashes),
  is_dirty_(false),
  is_owned_(true),
  is_read_only_(false),
  capacity_bits_(((num_bits + 63) >> 6) << 6), // can round to nearest multiple of 64 prior to bounds checks
  num_bits_set_(0)
  {
    if (num_hashes == 0) {
      throw std::invalid_argument("Must have at least 1 hash function");
    }
    if (num_bits == 0) {
      throw std::invalid_argument("Number of bits must be greater than zero");
    } else if (num_bits > MAX_FILTER_SIZE_BITS) {
      throw std::invalid_argument("Filter may not exceed " + std::to_string(MAX_FILTER_SIZE_BITS) + " bits");
    }

    const uint64_t num_bytes = capacity_bits_ >> 3;
    bit_array_ = allocator_.allocate(num_bytes);
    std::fill_n(bit_array_, num_bytes, 0);
    if (bit_array_ == nullptr) {
      throw std::bad_alloc();
    }
    memory_ = nullptr;
  }

template<typename A>
bloom_filter_alloc<A>::~bloom_filter_alloc() {
  // TODO: handle when only bit_array_ is used
  // TODO: handle when memory_ is used and bit_array_ is a pointer into it
  /*
  if (is_owned_ && memory_ != nullptr) {
    allocator_.deallocate(memory_, capacity_bits_ >> 3);
    memory_ = nullptr;
    bit_array_ = nullptr; // just to be safe
  }
  */
  if (memory_ == nullptr && bit_array_ != nullptr) {
    allocator_.deallocate(bit_array_, capacity_bits_ >> 3);
    bit_array_ = nullptr;
  }
}

template<typename A>
bool bloom_filter_alloc<A>::is_empty() const {
  return !is_dirty_ && num_bits_set_ == 0;
}

template<typename A>
uint64_t bloom_filter_alloc<A>::get_bits_used() {
  if (is_dirty_) {
    num_bits_set_ = bit_array_ops::count_num_bits_set(bit_array_, capacity_bits_ >> 3);
    is_dirty_ = false;
  }
  return num_bits_set_;
}

template<typename A>
uint64_t bloom_filter_alloc<A>::get_capacity() const {
  return capacity_bits_;
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
  // TODO: if wrapped, update num_bits_set in memory, too
  num_bits_set_ = 0;
  is_dirty_ = false;
  std::fill(bit_array_, bit_array_ + (capacity_bits_ >> 3), 0);
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
  ldu.double_value = static_cast<double>(item);
  if (item == 0.0) {
    ldu.double_value = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(ldu.double_value)) {
    ldu.long_value = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
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
  const uint64_t num_bits = get_capacity();
  for (uint16_t i = 1; i <= num_hashes_; i++) {
    const uint64_t hash_index = ((h0 + i * h1) >> 1) % num_bits;
    bit_array_ops::set_bit(bit_array_, hash_index);
  }
  is_dirty_ = true;
}

// QUERY-AND-UPDATE METHODS

template<typename A>
bool bloom_filter_alloc<A>::query_and_update(const std::string& item) {
  if (item.empty()) return false;
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
  ldu.double_value = item;
  if (item == 0.0) {
    ldu.double_value = 0.0; // canonicalize -0.0 to 0.0
  } else if (std::isnan(ldu.double_value)) {
    ldu.long_value = 0x7ff8000000000000L; // canonicalize NaN using value from Java's Double.doubleToLongBits()
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
  if (item == nullptr || size == 0) return false;
  const uint64_t h0 = XXHash64::hash(item, size, seed_);
  const uint64_t h1 = XXHash64::hash(item, size, h0);
  return internal_query_and_update(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::internal_query_and_update(const uint64_t h0, const uint64_t h1) {
  const uint64_t num_bits = get_capacity();
  bool value_exists = true;
  for (uint16_t i = 1; i <= num_hashes_; i++) {
    const uint64_t hash_index = ((h0 + i * h1) >> 1) % num_bits;
    bool value = bit_array_ops::get_and_set_bit(bit_array_, hash_index);
    num_bits_set_ += value ? 0 : 1;
    value_exists &= value;
  }
  return value_exists;
}

// QUERY METHODS

template<typename A>
bool bloom_filter_alloc<A>::query(const std::string& item) const {
  if (item.empty()) return false;
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
  if (item == nullptr || size == 0) return false;
  const uint64_t h0 = XXHash64::hash(item, size, seed_);
  const uint64_t h1 = XXHash64::hash(item, size, h0);
  return internal_query(h0, h1);
}

template<typename A>
bool bloom_filter_alloc<A>::internal_query(const uint64_t h0, const uint64_t h1) const {
  const uint64_t num_bits = get_capacity();
  for (uint16_t i = 1; i <= num_hashes_; i++) {
    const uint64_t hash_index = ((h0 + i * h1) >> 1) % num_bits;
    if (!bit_array_ops::get_bit(bit_array_, hash_index))
      return false;
  }
  return true;
}

// OTHER METHODS

template<typename A>
bool bloom_filter_alloc<A>::is_compatible(const bloom_filter_alloc& other) const {
  return seed_ == other.seed_
    && num_hashes_ == other.num_hashes_
    && get_capacity() == other.get_capacity()
    ;
}

template<typename A>
void bloom_filter_alloc<A>::union_with(const bloom_filter_alloc& other) {
  if (!is_compatible(other)) {
    throw std::invalid_argument("Incompatible bloom filters");
  }
  num_bits_set_ = bit_array_ops::union_with(bit_array_, other.bit_array_, capacity_bits_ >> 3);
  is_dirty_ = false;
}

template<typename A>
void bloom_filter_alloc<A>::intersect(const bloom_filter_alloc& other) {
  if (!is_compatible(other)) {
    throw std::invalid_argument("Incompatible bloom filters");
  }
  num_bits_set_ = bit_array_ops::intersect(bit_array_, other.bit_array_, capacity_bits_ >> 3);
  is_dirty_ = false;
}

template<typename A>
void bloom_filter_alloc<A>::invert() {
  num_bits_set_ = bit_array_ops::invert(bit_array_, capacity_bits_ >> 3);
  is_dirty_ = false;
}

template<typename A>
string<A> bloom_filter_alloc<A>::to_string(bool print_filter) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream oss;
  oss << "### Bloom Filter Summary:" << std::endl;
  oss << "   num_bits   : " << get_capacity() << std::endl;
  oss << "   num_hashes : " << num_hashes_ << std::endl;
  oss << "   seed       : " << seed_ << std::endl;
  oss << "   bits_used  : " << get_bits_used() << std::endl;
  oss << "   fill %     : " << (get_bits_used() * 100.0) / get_capacity() << std::endl;
  oss << "### End filter summary" << std::endl;

  if (print_filter) {
    uint64_t num_blocks = capacity_bits_ >> 6; // groups of 64 bits
    for (uint64_t i = 0; i < num_blocks; ++i) {
      oss << i << ": ";
      for (uint64_t j = 0; j < 8; ++j) { // bytes w/in a block
        for (uint64_t b = 0; b < 8; ++b) { // bits w/in a byte
          oss << ((bit_array_[i * 8 + j] & (1 << b)) ? "1" : "0");
        }
        oss << " ";
      }
      oss << std::endl;
    }
    oss << std::endl;
  }

  oss << std::endl;
  return string<A>(oss.str(), allocator_);
}


} // namespace datasketches

#endif // _BLOOM_FILTER_IMPL_HPP_