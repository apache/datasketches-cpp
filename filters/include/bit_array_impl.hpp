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

#ifndef _BIT_ARRAY_IMPL_HPP_
#define _BIT_ARRAY_IMPL_HPP_

#include <bitset>
#include <memory>

#include "common_defs.hpp"
#include "bit_array.hpp"

namespace datasketches {

template<typename A>
bit_array_alloc<A>::bit_array_alloc(const uint64_t num_bits, const A& allocator) :
  allocator_(allocator),
  num_bits_set_(0),
  is_dirty_(false)
  {
  if (num_bits == 0) {
    throw std::invalid_argument("Number of bits must be greater than zero");
  } else if (num_bits >= (((1ULL << 31) - 1) * 64)) {
    throw std::invalid_argument("Bits must be representable in fewer than 2^31 64-bit values");
  }

  // round up to the nearest multiple of 64, in bytes
  data_ = std::vector<uint8_t, A>(((num_bits + 63) >> 6) << 3, 0, allocator);
}

template<typename A>
bool bit_array_alloc<A>::is_empty() const {
  return !is_dirty_ && num_bits_set_ == 0;
}

template<typename A>
bool bit_array_alloc<A>::is_dirty() const {
  return is_dirty_;
}

template<typename A>
bool bit_array_alloc<A>::get_bit(const uint64_t index) const {
  if (index >= data_.size() << 3) {
    throw std::out_of_range("Index out of range");
  }
  return (data_[index >> 3] & (1 << (index & 7))) != 0;
}

template<typename A>
void bit_array_alloc<A>::set_bit(const uint64_t index) {
  if (index >= (data_.size() << 3)) {
    std::cout << "index: " << index << ", size: " << data_.size() << ", size << 3: " << (data_.size() << 3) << std::endl;
    throw std::out_of_range("Index out of range");
  }
  data_[index >> 3] |= (1 << (index & 7));
  is_dirty_ = true;
}

template<typename A>
void bit_array_alloc<A>::clear_bit(const uint64_t index) {
  if (index >= data_.size() << 3) {
    throw std::out_of_range("Index out of range");
  }
  data_[index >> 3] &= ~(1 << (index & 7));
  is_dirty_ = true;
}

template<typename A>
void bit_array_alloc<A>::assign_bit(const uint64_t index, const bool value) {
  if (value) {
    set_bit(index);
  } else {
    clear_bit(index);
  }
}

template<typename A>
bool bit_array_alloc<A>::get_and_set_bit(const uint64_t index) {
  if (index >= data_.size() << 3) {
    throw std::out_of_range("Index out of range");
  }
  const uint64_t offset = index >> 3;
  const uint8_t mask = 1 << (index & 7);
  if ((data_[offset] & mask) != 0) {
    return true;
  } else {
    data_[offset] |= mask;
    ++num_bits_set_; // increment the number of bits set regardless of is_dirty_
    return false;
  }
}

template<typename A>
uint64_t bit_array_alloc<A>::get_num_bits_set() {
  if (is_dirty_) {
    num_bits_set_ = 0;

    // we rounded up to a multiple of 64 so we know we can use 64-bit operations
    const uint64_t* data64 = reinterpret_cast<const uint64_t*>(data_.data());
    // Calculate the number of 64-bit chunks
    uint64_t num_longs = data_.size() / 8; // 8 bytes per 64 bits
    for (uint64_t i = 0; i < num_longs; ++i) {
      // Wrap the 64-bit chunk with std::bitset for easy bit counting
      std::bitset<64> bits(data64[i]);
      num_bits_set_ += bits.count();
    }
    is_dirty_ = false;
  }
  return num_bits_set_;
}

template<typename A>
uint64_t bit_array_alloc<A>::get_capacity() const {
  return data_.size() << 3; // size in bits
}

template<typename A>
void bit_array_alloc<A>::reset()  {
  uint8_t* data = data_.data();
  std::fill(data, data + data_.size(), 0);
  num_bits_set_ = 0;
  is_dirty_ = false;
}

template<typename A>
void bit_array_alloc<A>::union_with(const bit_array_alloc<A>& other) {
  if (data_.size() != other.data_.size()) {
    throw std::invalid_argument("Cannot union bit arrays with unequal lengths");
  }

  num_bits_set_ = 0;
  for (uint64_t i = 0; i < data_.size(); ++i) {
    data_[i] |= other.data_[i];
    std::bitset<8> bits(data_[i]);
    num_bits_set_ += bits.count();
  }
  is_dirty_ = false;
}

template<typename A>
void bit_array_alloc<A>::intersect(const bit_array_alloc<A>& other) {
  if (data_.size() != other.data_.size()) {
   throw std::invalid_argument("Cannot intersect bit arrays with unequal lengths");
  }

  num_bits_set_ = 0;
  for (uint64_t i = 0; i < data_.size(); ++i) {
    data_[i] &= other.data_[i];
    std::bitset<8> bits(data_[i]);
    num_bits_set_ += bits.count();
  }
  is_dirty_ = false;
}

template<typename A>
void bit_array_alloc<A>::invert() {
  if (is_dirty_) {
    num_bits_set_ = 0;
    for (uint64_t i = 0; i < data_.size(); ++i) {
      data_[i] = ~data_[i];
      std::bitset<8> bits(data_[i]);
      num_bits_set_ += bits.count();
    }
    is_dirty_ = false;
  } else {
    for (uint64_t i = 0; i < data_.size(); ++i) {
      data_[i] = ~data_[i];
    }
    num_bits_set_ = get_capacity() - num_bits_set_;
  }
}

template<typename A>
A bit_array_alloc<A>::get_allocator() const {
  return allocator_;
}

template<typename A>
string<A> bit_array_alloc<A>::to_string() const {
  std::ostringstream oss;
  uint64_t num_blocks = data_.size() / 8; // groups of 64 bits
  for (uint64_t i = 0; i < num_blocks; ++i) {
    oss << i << ": ";
    for (uint64_t j = 0; j < 8; ++j) { // bytes w/in a block
      for (uint64_t b = 0; b < 8; ++b) { // bits w/in a byte
        oss << ((data_[i * 8 + j] & (1 << b)) ? "1" : "0");
      }
      oss << " ";
    }
    oss << std::endl;
  }
  oss << std::endl;
  return oss.str();
}

} // namespace datasketches

#endif // _BIT_ARRAY_IMPL_HPP_