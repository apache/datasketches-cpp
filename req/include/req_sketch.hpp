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

#ifndef REQ_SKETCH_HPP_
#define REQ_SKETCH_HPP_

#include "req_common.hpp"
#include "req_compactor.hpp"
#include "req_quantile_calculator.hpp"

namespace datasketches {

template<
  typename T,
  bool IsHighRank,
  typename Comparator = std::less<T>,
  typename SerDe = serde<T>,
  typename Allocator = std::allocator<T>
>
class req_sketch {
public:
  using Compactor = req_compactor<T, IsHighRank, Comparator, Allocator>;
  using AllocCompactor = typename std::allocator_traits<Allocator>::template rebind_alloc<Compactor>;

  explicit req_sketch(uint16_t k, const Allocator& allocator = Allocator());
  ~req_sketch();
  req_sketch(const req_sketch& other);
  req_sketch(req_sketch&& other) noexcept;
  req_sketch& operator=(const req_sketch& other);
  req_sketch& operator=(req_sketch&& other);

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  bool is_empty() const;

  /**
   * Returns the length of the input stream.
   * @return stream length
   */
  uint64_t get_n() const;

  /**
   * Returns the number of retained items in the sketch.
   * @return number of retained items
   */
  uint32_t get_num_retained() const;

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  bool is_estimation_mode() const;

  template<typename FwdT>
  void update(FwdT&& item);

  template<typename FwdSk>
  void merge(FwdSk&& other);

  /**
   * Returns the min value of the stream.
   * For floating point types: if the sketch is empty this returns NaN.
   * For other types: if the sketch is empty this throws runtime_error.
   * @return the min value of the stream
   */
  const T& get_min_value() const;

  /**
   * Returns the max value of the stream.
   * For floating point types: if the sketch is empty this returns NaN.
   * For other types: if the sketch is empty this throws runtime_error.
   * @return the max value of the stream
   */
  const T& get_max_value() const;

  /**
   * Returns an approximation to the normalized (fractional) rank of the given item from 0 to 1 inclusive.
   * With the template parameter inclusive=true the weight of the given item is included into the rank.
   * Otherwise the rank equals the sum of the weights of items less than the given item according to the Comparator.
   *
   * <p>If the sketch is empty this returns NaN.
   *
   * @param item to be ranked
   * @return an approximate rank of the given item
   */

  template<bool inclusive = false>
  double get_rank(const T& item) const;

  template<bool inclusive = false>
  const T& get_quantile(double rank) const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for fixed-size arithmetic types (integral and floating point).
   * @return size in bytes needed to serialize this sketch
   */
  template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes() const;

  /**
   * Computes size needed to serialize the current state of the sketch.
   * This version is for all other types and can be expensive since every item needs to be looked at.
   * @return size in bytes needed to serialize this sketch
   */
  template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
  size_t get_serialized_size_bytes() const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  // This is a convenience alias for users
  // The type returned by the following serialize method
  using vector_bytes = std::vector<uint8_t, typename std::allocator_traits<Allocator>::template rebind_alloc<uint8_t>>;

  /**
   * This method serializes the sketch as a vector of bytes.
   * An optional header can be reserved in front of the sketch.
   * It is a blank space of a given size.
   * This header is used in Datasketches PostgreSQL extension.
   * @param header_size_bytes space to reserve in front of the sketch
   */
  vector_bytes serialize(unsigned header_size_bytes = 0) const;

  /**
   * This method deserializes a sketch from a given stream.
   * @param is input stream
   * @return an instance of a sketch
   */
  static req_sketch deserialize(std::istream& is, const Allocator& allocator = Allocator());

  /**
   * This method deserializes a sketch from a given array of bytes.
   * @param bytes pointer to the array of bytes
   * @param size the size of the array
   * @return an instance of a sketch
   */
  static req_sketch deserialize(const void* bytes, size_t size, const Allocator& allocator = Allocator());

  /**
   * Prints a summary of the sketch.
   * @param print_levels if true include information about levels
   * @param print_items if true include sketch data
   */
  string<Allocator> to_string(bool print_levels = false, bool print_items = false) const;

private:
  Allocator allocator_;
  uint16_t k_;
  uint32_t max_nom_size_;
  uint32_t num_retained_;
  uint64_t n_;
  std::vector<Compactor, AllocCompactor> compactors_;
  T* min_value_;
  T* max_value_;

  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t FAMILY = 17;
  static const size_t PREAMBLE_SIZE_BYTES = 8;
  enum flags { RESERVED1, RESERVED2, IS_EMPTY, IS_HIGH_RANK, RAW_ITEMS, IS_LEVEL_ZERO_SORTED };

  uint8_t get_num_levels() const;
  void grow();
  void update_max_nom_size();
  void update_num_retained();
  void compress();

  // for deserialization
  class item_deleter;
  req_sketch(uint32_t k, uint64_t n, std::unique_ptr<T, item_deleter> min_value, std::unique_ptr<T, item_deleter> max_value, std::vector<Compactor, AllocCompactor>&& compactors);

  static void check_preamble_ints(uint8_t preamble_ints, uint8_t num_levels);
  static void check_serial_version(uint8_t serial_version);
  static void check_family_id(uint8_t family_id);

  // implementations for floating point types
  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static const TT& get_invalid_value() {
    static TT value = std::numeric_limits<TT>::quiet_NaN();
    return value;
  }

  template<typename TT = T, typename std::enable_if<std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_value(const TT& value) {
    return !std::isnan(value);
  }

  // implementations for all other types
  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static const TT& get_invalid_value() {
    throw std::runtime_error("getting quantiles from empty sketch is not supported for this type of values");
  }

  template<typename TT = T, typename std::enable_if<!std::is_floating_point<TT>::value, int>::type = 0>
  static inline bool check_update_value(const TT&) {
    return true;
  }

};

} /* namespace datasketches */

#include "req_sketch_impl.hpp"

#endif
