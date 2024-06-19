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

#ifndef QUOTIENT_FILTER_HPP_
#define QUOTIENT_FILTER_HPP_

#include <memory>
#include <tuple>

#include "common_defs.hpp"

namespace datasketches {

// forward declarations
template<typename A> class quotient_filter_alloc;

/// Quotient filter alias with default allocator
using quotient_filter = quotient_filter_alloc<std::allocator<uint8_t>>;

template<typename Allocator>
class quotient_filter_alloc {
public:
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>>;

  /**
   * @param lg_q
   * @param num_bits_per_entry length of remainder in bits + 3 metadata bits
   */
  explicit quotient_filter_alloc(uint8_t lg_q, uint8_t num_bits_per_entry, const Allocator& allocator = Allocator());

  /**
   * Update this filter with given unsigned 64-bit integer.
   * @param value uint64_t to update the filter with
   * @return true if the filter was updated
   */
  bool update(uint64_t value);

  /**
   * Update this filter with given data of any type.
   * This is a "universal" update that covers all cases above,
   * but may produce different hashes.
   * Be very careful to hash input values consistently using the same approach
   * both over time and on different platforms
   * and while passing filters between different languages.
   * For instance, for signed 32-bit values call update(int32_t) method above,
   * which does widening conversion to int64_t, if compatibility with Java is expected
   * @param data pointer to the data
   * @param length of the data in bytes
   * @return true if the filter was updated
   */
  bool update(const void* data, size_t length);

  /**
   * Queries the filter with the given unsigned 64-bit integer and returns whether
   * the value <em>might</em> have been seen previously. The filter's expected
   * False Positive Probability determines the chances of a true result being
   * a false positive. False negatives are never possible.
   * @param value uint64_t with which to query the filter
   * @return The result of querying the filter with the given value
   */
  bool query(uint64_t value) const;

  /**
   * Queries the filter with given data of any type.
   * This is a "universal" query that covers all cases above,
   * but may produce different hashes.
   * Be very careful to hash input values consistently using the same approach
   * both over time and on different platforms
   * and while passing filters between different languages.
   * For instance, for signed 32-bit values call update(int32_t) method above,
   * which does widening conversion to int64_t, if compatibility with Java is expected
   * @param data pointer to the data
   * @param length of the data in bytes
   * @return The result of querying the filter with the given value
   */
  bool query(const void* data, size_t length) const;

  size_t get_num_entries() const;

  uint8_t get_lg_q() const;

  uint8_t get_num_bits_per_entry() const;

  uint8_t get_num_bits_in_value() const;

  uint8_t get_num_expansions() const;

  /**
   * Returns an instance of the allocator for this filter.
   * @return allocator
   */
  Allocator get_allocator() const;

  /**
   * Provides a human-readable summary of this filter as a string
   * @param print_entries if true include the list of entries
   * @return filter summary as a string
   */
  string<Allocator> to_string(bool print_entries = false) const;

  void serialize(std::ostream& os) const;

private:
  Allocator allocator_;
  uint8_t lg_q_;
  uint8_t num_bits_per_entry_;
  uint8_t num_expansions_;
  size_t num_entries_;
  vector_bytes bytes_;

  static constexpr double LOAD_FACTOR = 0.9;

  inline size_t get_q() const;
  inline size_t get_slot_mask() const;
  inline uint64_t get_value_mask() const;

  inline size_t quotient_from_hash(uint64_t hash) const;
  inline uint64_t value_from_hash(uint64_t hash) const;

  inline bool get_bit(size_t bit_index) const;
  inline bool get_is_occupied(size_t slot) const;
  inline bool get_is_continuation(size_t slot) const;
  inline bool get_is_shifted(size_t slot) const;
  inline bool is_slot_empty(size_t slot) const;
  inline uint64_t get_value(size_t slot) const;
  inline void set_bit(size_t bit_index, bool state);
  inline void set_is_occupied(size_t slot, bool state);
  inline void set_is_continuation(size_t slot, bool state);
  inline void set_is_shifted(size_t slot, bool state);
  inline void set_value(size_t slot, uint64_t value);

  size_t find_run_start(size_t quotient) const;
  std::pair<size_t, bool> find_in_run(size_t run_start, uint64_t value) const;

  bool insert(size_t quotient, uint64_t value);
  void insert_and_shift(size_t quotient, size_t slot, uint64_t value, bool is_new_run, bool is_run_start);

  void expand();
};

} /* namespace datasketches */

#include "quotient_filter_impl.hpp"

#endif
