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

#ifndef _BIT_ARRAY_HPP_
#define _BIT_ARRAY_HPP_

#include <memory>
#include <vector>

#include "common_defs.hpp"

namespace datasketches {

// forward declarations
template<typename A> class bit_array_alloc;

/// bit_array alias with default allocator
using bit_array = bit_array_alloc<std::allocator<uint8_t>>;

/**
 * This class holds an array of bits suitable for use in a Bloom Filter.
 * The representation is not compressed and is designed to fit in a single array
 * in Java, meaning that the maximum number of bits is limited by the maximize
 * size of an array of longs.
 *
 * For compatibility with Java, rounds the number of bits up to the smallest multiple of 64
 * (one long) that is not smaller than the specified number.
 */
template<typename Allocator = std::allocator<uint8_t>>
class bit_array_alloc {
  using A = Allocator;

public:
  /**
   * Construct a bit array with the given number of bits.
   * @param numBits the number of bits to represent.
   */
  explicit bit_array_alloc(const uint64_t num_bits, const Allocator& allocator = Allocator());

  bool is_empty() const;

  bool is_dirty() const;

  /**
   * Get the value of a bit at the given index.
   * @param index the index of the bit to get
   * @return the value of the bit at the given index.
   */
  bool get_bit(const uint64_t index) const;

  /**
   * Set the bit at the given index to 1.
   * @param index the index of the bit to set.
   */
  void set_bit(const uint64_t index);

  /**
   * Set the bit at the given index to 0.
   * @param index the index of the bit to clear.
   */
  void clear_bit(const uint64_t index);

  /**
   * Assign the value of the bit at the given index.
   * @param index the index of the bit to set.
   */
  void assign_bit(const uint64_t index, const bool value);

  /**
   * Gets teh value of a bit at the specified index and sets it to true
   * @poaram index the index of the bit to get and set
   * @return the value of the bit at the specified index
   */
  bool get_and_set_bit(const uint64_t index);

  /**
   * @brief Gets the number of bits set in the bit array.
   * @return the number of bits set in the bit array.
   */
  uint64_t get_num_bits_set() const;

  /**
   * @brief Gets the number of bits set in the bit array.
   * @return the number of bits set in the bit array.
   */
  uint64_t get_num_bits_set();

  /**
   * Resets the bit_aray, setting all bits to 0.
   */
  void reset();

  /**
   * Gets the capacity of the bit array in bits.
   * @return the capacity of the bit array in bits.
   */
  uint64_t get_capacity() const;

  /**
   * Performs a union operation on this bit array with another bit array.
   * This operation modifies the current bit array to be the union of its original bits and the bits of the other array.
   * The union operation is equivalent to a bitwise OR operation between the two arrays.
   *
   * @param other The other bit array to union with this one.
   */
  void union_with(const bit_array_alloc& other);

  /**
   * Performs an intersection operation on this bit array with another bit array.
   * This operation modifies the current bit array to contain only the bits that are set in both this array and the other array.
   * The intersection operation is equivalent to a bitwise AND operation between the two arrays.
   *
   * @param other The other bit array to intersect with this one.
   */
  void intersect(const bit_array_alloc& other);

  /**
   * Inverts the bits of this bit array.
   * This operation modifies the current bit array by flipping all its bits; 0s become 1s and 1s become 0s.
   */
  void invert();

  /**
   * Returns a string representation of the bit_array
   * @return a string representation of the bit_array
   */
  string<A> to_string() const;

  /**
   * @brief Get the allocator object
   *
   * @return Allocator
   */
  Allocator get_allocator() const;

private:
  A allocator_;
  uint64_t num_bits_set_;  // if -1, need to recompute value
  bool is_dirty_;
  std::vector<uint8_t, A> data_;

  uint64_t count_bits_set() const;
};

} // namespace datasketches

#include "bit_array_impl.hpp"

#endif // _BIT_ARRAY_HPP_