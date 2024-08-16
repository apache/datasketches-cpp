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

#ifndef _BIT_ARRAY_OPS_HPP_
#define _BIT_ARRAY_OPS_HPP_

#include <bitset>

namespace datasketches {

/**
 * This class comprises methods that operate one or more arrays of bits (uint8_t*) to
 * provide bit array operations. The class does not take ownership of memory and operates On
 * arrays in-place. Sizes of the arrays, in bytes, are passed in as arguments.
 *
 * None of the methods in this class perform bounds checks. The caller is responsible for ensuring
 * that indices are within the array bounds.
 *
 * Implementation assumes the actual arrays are multiples of 64 bits in length.
 */
namespace bit_array_ops {

  /**
   * Get the value of a bit at the given index.
   * @param array the array of bits
   * @param index the index of the bit to get
   * @return the value of the bit at the given index.
   */
  static inline bool get_bit(uint8_t* array, uint64_t index) {
    return (array[index >> 3] & (1 << (index & 7))) != 0;
  }

  /**
   * Set the bit at the given index to 1.
   * @param array the array of bits
   * @param index the index of the bit to set.
   */
  static inline void set_bit(uint8_t* array, uint64_t index) {
    array[index >> 3] |= (1 << (index & 7));
  }

  /**
   * Set the bit at the given index to 0.
   * @param array the array of bits
   * @param index the index of the bit to clear.
   */
  static inline void clear_bit(uint8_t* array, uint64_t index) {
    array[index >> 3] &= ~(1 << (index & 7));
  }

  /**
   * Assign the value of the bit at the given index.
   * @param array the array of bits
   * @param index the index of the bit to set.
   */
  static inline void assign_bit(uint8_t* array, uint64_t index, bool value) {
    // read-only checks handled by set_bit() and clear_bit()
    if (value) {
      set_bit(array, index);
    } else {
      clear_bit(array, index);
    }
  }

  /**
   * Gets teh value of a bit at the specified index and sets it to true
   * @param array the array of bits
   * @param index the index of the bit to get and set
   * @return the value of the bit at the specified index
   */
  static inline bool get_and_set_bit(uint8_t* array, uint64_t index) {
    const uint64_t offset = index >> 3;
    const uint8_t mask = 1 << (index & 7);
    if ((array[offset] & mask) != 0) {
      return true;
    } else {
      array[offset] |= mask;
      return false;
    }
  }

  /**
   * @brief Gets the number of bits set in the bit array.
   * @param array the array of bits
   * @param length_bytes the length of the array, in bytes
   * @return the number of bits set in the bit array.
   */
  static inline uint64_t count_num_bits_set(uint8_t* array, uint64_t length_bytes) {
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

  /**
   * Performs a union operation on one bit array with another bit array.
   * This operation modifies the tgt bit array to be the union of its original bits and the bits of the src array.
   * The union operation is equivalent to a bitwise OR operation between the two arrays.
   *
   * @param tgt the array of bits into which the results are written
   * @param src the array of bits to union into tgt
   * @param length_bytes the length of the two arrays, in bytes
   * @return the number of bits set in the resulting array
   */
  static inline uint64_t union_with(uint8_t* tgt, const uint8_t* src, uint64_t length_bytes) {
    uint64_t num_bits_set = 0;
    for (uint64_t i = 0; i < length_bytes; ++i) {
      tgt[i] |= src[i];
      std::bitset<8> bits(tgt[i]);
      num_bits_set += bits.count();
    }
    return num_bits_set;
  }

  /**
   * Performs an intersection operation on one bit array with another bit array.
   * This operation modifies the tgt bit array to contain only the bits that are set in both that array and the src array.
   * The intersection operation is equivalent to a bitwise AND operation between the two arrays.
   *
   * @param tgt the array of bits into which the results are written
   * @param src the array of bits to intersect with tgt
   * @param length_bytes the length of the two arrays, in bytes
   * @return the number of bits set in the resulting array
   */
  static inline uint64_t intersect(uint8_t* tgt, const uint8_t* src, uint64_t length_bytes) {
    uint64_t num_bits_set = 0;
    for (uint64_t i = 0; i < length_bytes; ++i) {
      tgt[i] &= src[i];
      std::bitset<8> bits(tgt[i]);
      num_bits_set += bits.count();
    }
    return num_bits_set;
  }

  /**
   * Inverts the bits of this bit array.
   * This operation modifies the bit array by flipping all its bits; 0s become 1s and 1s become 0s.
   * @param array the array of bits
   * @param length_bytes the length of the array, in bytes
   * @return the number of bits set in the resulting array
   */
  static inline uint64_t invert(uint8_t* array, uint64_t length_bytes) {
    uint64_t num_bits_set = 0;
    for (uint64_t i = 0; i < length_bytes; ++i) {
      array[i] = ~array[i];
      std::bitset<8> bits(array[i]);
      num_bits_set += bits.count();
    }
    return num_bits_set;
  }

} // namespace bit_array_ops

} // namespace datasketches

#endif // _BIT_ARRAY_OPS_HPP_
