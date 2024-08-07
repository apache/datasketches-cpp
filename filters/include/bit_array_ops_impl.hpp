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

#ifndef _BIT_ARRAY_OPS_IMPL_HPP_
#define _BIT_ARRAY_OPS_IMPL_HPP_

#include <bitset>

#include "bit_array_ops.hpp"

namespace datasketches {

bool bit_array_ops::get_bit(uint8_t* array, const uint64_t index) {
  return (array[index >> 3] & (1 << (index & 7))) != 0;
}

void bit_array_ops::set_bit(uint8_t* array, const uint64_t index) {
  array[index >> 3] |= (1 << (index & 7));
}

void bit_array_ops::clear_bit(uint8_t* array, const uint64_t index) {
  array[index >> 3] &= ~(1 << (index & 7));
}

void bit_array_ops::assign_bit(uint8_t* array, const uint64_t index, const bool value) {
  // read-only checks handled by set_bit() and clear_bit()
  if (value) {
    set_bit(array, index);
  } else {
    clear_bit(array, index);
  }
}

bool bit_array_ops::get_and_set_bit(uint8_t* array, const uint64_t index) {
  const uint64_t offset = index >> 3;
  const uint8_t mask = 1 << (index & 7);
  if ((array[offset] & mask) != 0) {
    return true;
  } else {
    array[offset] |= mask;
    return false;
  }
}

uint64_t bit_array_ops::count_num_bits_set(uint8_t* array, const uint64_t length_bytes) {
  uint64_t num_bits_set = 0;

  // we rounded up to a multiple of 64 so we know we can use 64-bit operations
  const uint64_t* array64 = reinterpret_cast<const uint64_t*>(array);
  // Calculate the number of 64-bit chunks
  uint64_t num_longs = length_bytes / 8; // 8 bytes per 64 bits
  for (uint64_t i = 0; i < num_longs; ++i) {
    // Wrap the 64-bit chunk with std::bitset for easy bit counting
    std::bitset<64> bits(array64[i]);
    num_bits_set += bits.count();
  }
  return num_bits_set;
}

uint64_t bit_array_ops::union_with(uint8_t* tgt, const uint8_t* src, const uint64_t length_bytes) {
  uint64_t num_bits_set = 0;
  for (uint64_t i = 0; i < length_bytes; ++i) {
    tgt[i] |= src[i];
    std::bitset<8> bits(tgt[i]);
    num_bits_set += bits.count();
  }
  return num_bits_set;
}

uint64_t bit_array_ops::intersect(uint8_t* tgt, const uint8_t* src, const uint64_t length_bytes) {
  uint64_t num_bits_set = 0;
  for (uint64_t i = 0; i < length_bytes; ++i) {
    tgt[i] &= src[i];
    std::bitset<8> bits(tgt[i]);
    num_bits_set += bits.count();
  }
  return num_bits_set;
}

uint64_t bit_array_ops::invert(uint8_t* array, const uint64_t length_bytes) {
  uint64_t num_bits_set = 0;
  for (uint64_t i = 0; i < length_bytes; ++i) {
    array[i] = ~array[i];
    std::bitset<8> bits(array[i]);
    num_bits_set += bits.count();
  }
  return num_bits_set;
}

} // namespace datasketches

#endif // _BIT_ARRAY_OPS_IMPL_HPP_