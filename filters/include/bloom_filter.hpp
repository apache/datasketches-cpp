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

#ifndef _BLOOM_FILTER_HPP_
#define _BLOOM_FILTER_HPP_

#include <cstdint>
#include <memory>
#include <vector>

#include "common_defs.hpp"

namespace datasketches {

// forward declarations
template<typename A> class bloom_filter_alloc;
template<typename A> class bloom_filter_builder_alloc;

// aliases with default allocator
using bloom_filter = bloom_filter_alloc<std::allocator<uint8_t>>;
using bloom_filter_builder = bloom_filter_builder_alloc<std::allocator<uint8_t>>;

/**
 * <p>This class provides methods to help estimate the correct parameters when
 * creating a Bloom filter, and methods to create the filter using those values.</p>
 *
 * <p>The underlying math is described in the
 * <a href='https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions'>
 * Wikipedia article on Bloom filters</a>.</p>
 */
template<typename Allocator = std::allocator<uint8_t>>
class bloom_filter_builder_alloc {
public:
  /**
   * Returns the optimal number of hash functions to given target numbers of distinct items
   * and the Bloom filter size in bits. This function will provide a result even if the input
   * values exceed the capacity of a single Bloom filter.
   * @param max_distinct_items The maximum expected number of distinct items to add to the filter
   * @param num_filter_bits The intended size of the Bloom Filter in bits
   * @return The suggested number of hash functions to use with the filter
   */
  static uint16_t suggest_num_hashes(uint64_t max_distinct_items, uint64_t num_filter_bits);

  /**
   * Returns the optimal number of hash functions to achieve a target false positive probability.
   * @param target_false_positive_prob A desired false positive probability per item
   * @return The suggested number of hash functions to use with the filter.
   */
  static uint16_t suggest_num_hashes(double target_false_positive_prob);

  /**
   * Returns the optimal number of bits to use in a Bloom filter given a target number of distinct
   * items and a target false positive probability.
   * @param max_distinct_items The maximum expected number of distinct items to add to the filter
   * @param target_false_positive_prob A desired false positive probability per item
   * @return The suggested number of bits to use with the filter
   */
  static uint64_t suggest_num_filter_bits(uint64_t max_distinct_items, double target_false_positive_prob);

  /**
   * Creates a new Bloom filter with an optimal number of bits and hash functions for the given inputs,
   * using a random base seed for the hash function.
   * @param max_distinct_items The maximum expected number of distinct items to add to the filter
   * @param target_false_positive_prob A desired false positive probability per item
   * @param seed A bash hash seed (default: random)
   * @param allocator The allocator to use for the filter (default: standard allocator)
   * @return A new Bloom filter configured for the given input parameters
   */
  static bloom_filter_alloc<Allocator> create_by_accuracy(uint64_t max_distinct_items,
                                                          double target_false_positive_prob,
                                                          uint64_t seed = generate_random_seed(),
                                                          const Allocator& allocator = Allocator());

  /**
   * Creates a Bloom filter with given number of bits and number of hash functions,
   * using the provided base seed for the hash function.
   *
   * @param num_bits The size of the BloomFilter, in bits
   * @param num_hashes The number of hash functions to apply to items
   * @param seed A base hash seed (default: random)
   * @param allocator The allocator to use for the filter (default: standard allocator)
   * @return A new Bloom filter configured for the given input parameters
   */
  static bloom_filter_alloc<Allocator> create_by_size(uint64_t num_bits,
                                                      uint16_t num_hashes,
                                                      uint64_t seed = generate_random_seed(),
                                                      const Allocator& allocator = Allocator());

  /**
   * Creates a new Bloom filter with an optimal number of bits and hash functions for the given inputs,
   * using a random base seed for the hash function and writing into the provided memory. The filter does
   * not take ownership of the memory but does overwrite the full contents.
   *
   * @param memory A pointer to the memory to use for the filter
   * @param length_bytes The length of the memory in bytes
   * @param max_distinct_items The maximum expected number of distinct items to add to the filter
   * @param target_false_positive_prob A desired false positive probability per item
   * @param dstMem A WritableMemory to hold the initialized filter
   * @param allocator The allocator to use for the filter (default: standard allocator)
   * @return A new Bloom filter configured for the given input parameters in the provided memory
   */
  static bloom_filter_alloc<Allocator> initialize_by_accuracy(void* memory,
                                                              size_t length_bytes,
                                                              uint64_t max_distinct_items,
                                                              double target_false_positive_prob,
                                                              uint64_t seed = generate_random_seed(),
                                                              const Allocator& allocator = Allocator());

  /**
   * Initializes a Bloom filter with given number of bits and number of hash functions,
   * using the provided base seed for the hash function and writing into the provided memory. The filter does
   * not take ownership of the memory but does overwrite the full contents.
   *
   * @param memory A pointer to the memory to use for the filter
   * @param length_bytes The length of the memory in bytes
   * @param num_bits The size of the BloomFilter, in bits
   * @param num_hashes The number of hash functions to apply to items
   * @param seed A base hash seed (default: random)
   * @param allocator The allocator to use for the filter (default: standard allocator)
   * @return A new BloomFilter configured for the given input parameters
   */
  static bloom_filter_alloc<Allocator> initialize_by_size(void* memory,
                                                          size_t length_bytes,
                                                          uint64_t num_bits,
                                                          uint16_t num_hashes,
                                                          uint64_t seed = generate_random_seed(),
                                                          const Allocator& allocator = Allocator());

  /**
   * @brief Generates a random 64-bit seed value
   *
   * @return uint64_t a random value over the range of unsigned 64-bit integers
   */
  static uint64_t generate_random_seed();

private:
  static void validate_size_inputs(uint64_t num_bits, uint16_t num_hashes);
  static void validate_accuracy_inputs(uint64_t max_distinct_items, double target_false_positive_prob);
};

/**
 * <p>A Bloom filter is a data structure that can be used for probabilistic
 * set membership.</p>
 *
 * <p>When querying a Bloom filter, there are no false positives. Specifically:
 * When querying an item that has already been inserted to the filter, the filter will
 * always indicate that the item is present. There is a chance of false positives, where
 * querying an item that has never been presented to the filter will indicate that the
 * item has already been seen. Consequently, any query should be interpreted as
 * "might have seen."</p>
 *
 * <p>A standard Bloom filter is unlike typical sketches in that it is not sub-linear
 * in size and does not resize itself. A Bloom filter will work up to a target number of
 * distinct items, beyond which it will saturate and the false positive rate will start to
 * increase. The size of a Bloom filter will be linear in the expected number of
 * distinct items.</p>
 *
 * <p>See the bloom_filter_builder_alloc class for methods to create a filter, especially
 * one sized correctly for a target number of distinct elements and a target
 * false positive probability.</p>
 *
 * <p>This implementation uses xxHash64 and follows the approach in Kirsch and Mitzenmacher,
 * "Less Hashing, Same Performance: Building a Better Bloom Filter," Wiley Interscience, 2008, pp. 187-218.</p>
 */

template<typename Allocator = std::allocator<uint8_t>>
class bloom_filter_alloc {
public:

  /**
   * This method deserializes a Bloom filter from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @param allocator instance of an Allocator
   * @return an instance of a Bloom filter
   */
  static bloom_filter_alloc deserialize(const void* bytes, size_t length_bytes, const Allocator& allocator = Allocator());

  /**
   * This method deserializes a Bloom filter from a given stream.
   * @param is input stream
   * @param allocator instance of an Allocator
   * @return an instance of a Bloom filter
   */
  static bloom_filter_alloc deserialize(std::istream& is, const Allocator& allocator = Allocator());

  /**
   * @brief Wraps the provided memory as a read-only Bloom filter. Reads the data in-place and does
   * not take ownership of the underlying memory. Does not allow modifying the filter.
   *
   * @param data The memory to wrap
   * @param length_bytes The length of the memory in bytes
   * @param allocator instance of an Allocator
   * @return a const (read-only) Bloom filter wrapping the provided memory
   */
  static const bloom_filter_alloc wrap(const void* data, size_t length_bytes, const Allocator& allocator = Allocator());

  /**
   * @brief Wraps the provided memory as a writable Bloom filter. Reads the data in-place and does
   * not take ownership of the underlying memory. Allows modifying the filter.
   *
   * @param data the memory to wrap
   * @param length_bytes the length of the memory in bytes
   * @param allocator instance of an Allocator
   * @return a Bloom filter wrapping the provided memory
   */
  static bloom_filter_alloc writable_wrap(void* data, size_t length_bytes, const Allocator& allocator = Allocator());

  /**
   * Copy constructor
   * @param other filter to be copied
   */
  bloom_filter_alloc(const bloom_filter_alloc& other);

  /** Move constructor
   * @param other filter to be moved
   */
  bloom_filter_alloc(bloom_filter_alloc&& other) noexcept;

  /**
   * Copy assignment
   * @param other filter to be copied
   * @return reference to this filter
   */
  bloom_filter_alloc& operator=(const bloom_filter_alloc& other);

  /**
   * Move assignment
   * @param other filter to be moved
   * @return reference to this filter
   */
  bloom_filter_alloc& operator=(bloom_filter_alloc&& other);

  /**
   * @brief Destroy the bloom filter object
   */
  ~bloom_filter_alloc();

  // This is a convenience alias for users
  // The type returned by the following serialize method
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>>;

  /**
   * This method serializes the filter as a vector of bytes.
   * An optional header can be reserved in front of the filter.
   * It is a blank space of a given size.
   * Some integrations such as PostgreSQL may need this header space.
   * @param header_size_bytes space to reserve in front of the filter
   * @return serialized filter as a vector of bytes
   */
  vector_bytes serialize(unsigned header_size_bytes = 0) const;

  /**
   * This method serializes the filter into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  /**
   * Checks if the Bloom Filter has processed any items
   * @return True if the BloomFilter is empty, otherwise False
   */
  bool is_empty() const;

  /**
   * Returns the number of bits in the Bloom Filter that are set to 1.
   * @return The number of bits in use in this filter
   */
  uint64_t get_bits_used();

  /**
   * Returns the total number of bits in the Bloom Filter.
   * @return The total size of the Bloom Filter
   */
  uint64_t get_capacity() const;

  /**
   * Returns the configured number of hash functions for this Bloom Filter
   * @return The number of hash functions to apply to inputs
   */
  uint16_t get_num_hashes() const;

  /**
   * Returns the hash seed for this Bloom Filter.
   * @return The hash seed for this filter
   */
  uint64_t get_seed() const;

  /**
   * Resets the Bloom Filter to its original state.
   */
  void reset();

  // UPDATE METHODS

  /**
   * Updates the filter with the given std::string.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns.
   * @param item The given string.
   */
  void update(const std::string& item);

  /**
   * Updates the filter with the given unsigned 64-bit integer.
   * @param item The given integer.
   */
  void update(uint64_t item);

  /**
   * Updates the filter with the given unsigned 32-bit integer.
   * @param item The given integer.
   */
  void update(uint32_t item);

  /**
   * Updates the filter with the given unsigned 16-bit integer.
   * @param item The given integer.
   */
  void update(uint16_t item);

  /**
   * Updates the filter with the given unsigned 8-bit integer.
   * @param item The given integer.
   */
  void update(uint8_t item);

  /**
   * Updates the filter with the given signed 64-bit integer.
   * @param item The given integer.
   */
  void update(int64_t item);

  /**
   * Updates the filter with the given signed 32-bit integer.
   * @param item The given integer.
   */
  void update(int32_t item);

  /**
   * Updates the filter with the given signed 16-bit integer.
   * @param item The given integer.
   */
  void update(int16_t item);

  /**
   * Updates the filter with the given signed 8-bit integer.
   * @param item The given integer.
   */
  void update(int8_t item);

  /**
   * Updates the filter with the given 64-bit floating point value.
   * @param item The given double.
   */
  void update(double item);

  /**
   * Updates the filter with the give 32-bit floating point value.
   * @param item The given float.
   */
  void update(float item);

  /**
   * Updates the filter with the given data array.
   * @param data The given array.
   * @param length_bytes The array length in bytes.
   */
  void update(const void* data, size_t length_bytes);

  // QUERY-AND-UPDATE METHODS

  /**
   * Updates the filter with the given std::string and returns the result from
   * querying the filter prior to the update.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty no update attempt is made and the method returns false.
   * @param item The given string.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(const std::string& item);

  /**
   * Updates the filter with the given unsigned 64-bit integer and returns the result from
   * querying the filter prior to the update.
   * @param item The given integer.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(uint64_t item);

  /**
   * Updates the filter with the given unsigned 32-bit integer and returns the result from
   * querying the filter prior to the update.
   * @param item The given integer.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(uint32_t item);

  /**
   * Updates the filter with the given unsigned 16-bit integer and returns the result from
   * querying the filter prior to the update.
   * @param item The given integer.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(uint16_t item);

  /**
   * Updates the filter with the given unsigned 8-bit integer and returns the result from
   * querying the filter prior to the update.
   * @param item The given integer.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(uint8_t item);

  /**
   * Updates the filter with the given signed 64-bit integer and returns the result from
   * querying the filter prior to the update.
   * @param item The given integer.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(int64_t item);

  /**
   * Updates the filter with the given signed 32-bit integer and returns the result from
   * querying the filter prior to the update.
   * @param item The given integer.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(int32_t item);

  /**
   * Updates the filter with the given signed 16-bit integer and returns the result from
   * querying the filter prior to the update.
   * @param item The given integer.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(int16_t item);

  /**
   * Updates the filter with the given signed 8-bit integer and returns the result from
   * querying the filter prior to the update.
   * @param item The given integer.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(int8_t item);

  /**
   * Updates the filter with the given 64-bit floating point value and returns the result from
   * querying the filter prior to the update.
   * @param item The given double.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(double item);

  /**
   * Updates the filter with the give 32-bit floating point value and returns the result from
   * querying the filter prior to the update.
   * @param item The given float.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(float item);

  /**
   * Updates the filter with the given data array and returns the result from
   * querying the filter prior to the update.
   * @param data The given array.
   * @param length_bytes The array length in bytes.
   * @return The result from querying the filter prior to the update.
   */
  bool query_and_update(const void* data, size_t length_bytes);

  // QUERY METHODS

  /**
   * Queries the filter with the given std::string and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * The string is converted to a byte array using UTF8 encoding.
   * If the string is null or empty the method always returns false.
   * @param item The given string.
   * @return The result from querying the filter with the given item.
   */
  bool query(const std::string& item) const;

  /**
   * Queries the filter with the given unsigned 64-bit integer and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given integer.
   * @return The result from querying the filter with the given item.
   */
  bool query(uint64_t item) const;

  /**
   * Queries the filter with the given unsigned 32-bit integer and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given integer.
   * @return The result from querying the filter with the given item.
   */
  bool query(uint32_t item) const;

  /**
   * Queries the filter with the given unsigned 16-bit integer and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given integer.
   * @return The result from querying the filter with the given item.
   */
  bool query(uint16_t item) const;

  /**
   * Queries the filter with the given unsigned 8-bit integer and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given integer.
   * @return The result from querying the filter with the given item.
   */
  bool query(uint8_t item) const;

  /**
   * Queries the filter with the given signed 64-bit integer and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given integer.
   * @return The result from querying the filter with the given item.
   */
  bool query(int64_t item) const;

  /**
   * Queries the filter with the given signed 32-bit integer and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given integer.
   * @return The result from querying the filter with the given item.
   */
  bool query(int32_t item) const;

  /**
   * Queries the filter with the given signed 16-bit integer and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given integer.
   * @return The result from querying the filter with the given item.
   */
  bool query(int16_t item) const;

  /**
   * Queries the filter with the given signed 8-bit integer and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given integer.
   * @return The result from querying the filter with the given item.
   */
  bool query(int8_t item) const;

  /**
   * Queries the filter with the given 64-bit floating point value and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given double.
   * @return The result from querying the filter with the given item.
   */
  bool query(double item) const;

  /**
   * Queries the filter with the given 32-bit floating point value and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param item The given float.
   * @return The result from querying the filter with the given item.
   */
  bool query(float item) const;

  /**
   * Queries the filter with the given data array. and returns the result from
   * Queries the filter with the given 64-bit floating point value and returns whether the value
   * might have been seen previoiusly. The filter's expected Fale Positive Probability
   * determines the chances of a true result being a false positive. False engatives are
   * never possible.
   * @param data The given array.
   * @param length_bytes The array length in bytes.
   * @return The result from querying the filter with the given item.
   */
  bool query(const void* data, size_t length_bytes) const;

  // OTHER OPERATIONS

  /**
   * Unions two Bloom Filters by applying a logical OR. The result will recognized
   * any values seen by either filter (as well as false positives).
   * @param other A BloomFilter to union with this one
   */
  void union_with(const bloom_filter_alloc& other);

  /**
   * Intersects two Bloom Filters by applying a logical AND. The result will recognize
   * only values seen by both filters (as well as false positives).
   * @param other A Bloom Filter to union with this one
   */
  void intersect(const bloom_filter_alloc& other);

  /**
   * Inverts all the bits of the BloomFilter. Approximately inverts the notion of set-membership.
   */
  void invert();

  /**
   * Helps identify if two Bloom Filters may be unioned or intersected.
   * @param other A Bloom Filter to check for compatibility with this one
   * @return True if the filters are compatible, otherwise false
   */
   bool is_compatible(const bloom_filter_alloc& other) const;

  /**
   * @brief Checks if the Bloom Filter is read-only.
   *
   * @return True if the filter is read-only, otherwise false.
   */
  bool is_read_only() const;

  /**
   * @brief Returns whether the filter owns its underlying memory
   * @return True if the filter owns its memory, otherwise false
   */
  bool is_memory_owned() const;

  /**
   * @brief Checks if the Bloom Filter was created by a call to wrap().
   *
   * @return True if the filter was created by wrapping memory, otherwise false.
   */
  bool is_wrapped() const;

  /**
   * @brief Returns a pointer to the memory this filter wraps, if it exists.
   * @return A pointer to the wrapped memory, or nullptr if is_wrapped() is false.
   */
  const uint8_t* get_wrapped_memory() const;

  /**
   * @brief Gets the serialized size of the Bloom Filter in bytes
   * @return The serialized size of the Bloom Filter in bytes
   */
  size_t get_serialized_size_bytes() const;

  /**
   * @brief Gets the serialized size of the Bloom Filter with the given number of bits, in bytes
   * @param num_bits The number of bits in the Bloom Filter for the size calculation
   * @return The serialized size of a Bloom Filter with a capacity of num_bits, in bytes
   */
  static size_t get_serialized_size_bytes(uint64_t num_bits);

  /**
   * @brief Returns a human-readable string representation of the Bloom Filter.
   * @param print_filter If true, the filter bits will be printed as well.
   * @return A human-readable string representation of the Bloom Filter.
   */
  string<Allocator> to_string(bool print_filter = false) const;

private:
  using A = Allocator;
  using AllocUint8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;

  static const uint64_t DIRTY_BITS_VALUE = static_cast<uint64_t>(-1LL);
  static const uint64_t MAX_HEADER_SIZE_BYTES = 32; // 4 Java Longs
  static const uint64_t BIT_ARRAY_LENGTH_OFFSET_BYTES = 16;
  static const uint64_t NUM_BITS_SET_OFFSET_BYTES = 24;
  static const uint64_t BIT_ARRAY_OFFSET_BYTES = 32;
  static const uint64_t MAX_FILTER_SIZE_BITS = (INT32_MAX - MAX_HEADER_SIZE_BYTES) * sizeof(uint64_t);

  static const uint8_t PREAMBLE_LONGS_EMPTY = 3;
  static const uint8_t PREAMBLE_LONGS_STANDARD = 4;
  static const uint8_t FAMILY_ID = 21;
  static const uint8_t SER_VER = 1;
  static const uint8_t EMPTY_FLAG_MASK = 4;

  // used by builder methods
  bloom_filter_alloc(uint64_t num_bits, uint16_t num_hashes, uint64_t seed, const A& allocator);
  bloom_filter_alloc(uint8_t* memory, size_t length_bytes, uint64_t num_bits, uint16_t num_hashes, uint64_t seed, const A& allocator);

  // used by deserialize and wrap
  bloom_filter_alloc(uint64_t seed,
                     uint16_t num_hashes,
                     bool is_dirty,
                     bool is_owned,
                     bool is_read_only,
                     uint64_t capacity_bits,
                     uint64_t num_bits_set,
                     uint8_t* bit_array,
                     uint8_t* memory,
                     const A& allocator);

  static bloom_filter_alloc internal_deserialize_or_wrap(void* bytes,
                                                         size_t length_bytes,
                                                         bool read_only,
                                                         bool wrap,
                                                         const A& allocator);

  // internal query/update methods
  void internal_update(uint64_t h0, uint64_t h1);
  bool internal_query_and_update(uint64_t h0, uint64_t h1);
  bool internal_query(uint64_t h0, uint64_t h1) const;

  void update_num_bits_set(uint64_t num_bits_set);

  Allocator allocator_;
  uint64_t seed_;
  uint16_t num_hashes_;
  bool is_dirty_;
  bool is_owned_; // if true, data is not owned by filter AND memory_ holds the entire filter not just the bit array
  bool is_read_only_; // if true, filter is read-only
  uint64_t capacity_bits_;
  uint64_t num_bits_set_;
  uint8_t* bit_array_;  // data backing bit_array_, regardless of ownership
  uint8_t* memory_; // if wrapped, pointer to the start of the filter, otheriwse nullptr

  friend class bloom_filter_builder_alloc<A>;
};

} // namespace datasketches

#include "bloom_filter_builder_impl.hpp"
#include "bloom_filter_impl.hpp"

#endif // _BLOOM_FILTER_HPP_ b
