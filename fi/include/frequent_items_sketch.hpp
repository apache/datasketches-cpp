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

#ifndef FREQUENT_ITEMS_SKETCH_HPP_
#define FREQUENT_ITEMS_SKETCH_HPP_

#include <memory>
#include <vector>
#include <iostream>
#include <functional>

#include "reverse_purge_hash_map.hpp"
#include "serde.hpp"

namespace datasketches {

/*
 * Based on Java implementation here:
 * https://github.com/DataSketches/sketches-core/blob/master/src/main/java/com/yahoo/sketches/frequencies/ItemsSketch.java
 * author Alexander Saydakov
 */

enum frequent_items_error_type { NO_FALSE_POSITIVES, NO_FALSE_NEGATIVES };

// for serialization as raw bytes
template<typename A> using AllocU8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;
template<typename A> using vector_u8 = std::vector<uint8_t, AllocU8<A>>;

// type W for weight must be an arithmetic type (integral or floating point)
template<typename T, typename W = uint64_t, typename H = std::hash<T>, typename E = std::equal_to<T>, typename S = serde<T>, typename A = std::allocator<T>>
class frequent_items_sketch {
public:
  explicit frequent_items_sketch(uint8_t lg_max_map_size);
  frequent_items_sketch(uint8_t lg_start_map_size, uint8_t lg_max_map_size);
  class row;
  void update(const T& item, W weight = 1);
  void update(T&& item, W weight = 1);
  void merge(const frequent_items_sketch& other);
  void merge(frequent_items_sketch&& other);
  bool is_empty() const;
  uint32_t get_num_active_items() const;
  W get_total_weight() const;
  W get_estimate(const T& item) const;
  W get_lower_bound(const T& item) const;
  W get_upper_bound(const T& item) const;
  W get_maximum_error() const;
  double get_epsilon() const;
  static double get_epsilon(uint8_t lg_max_map_size);
  static double get_apriori_error(uint8_t lg_max_map_size, W estimated_total_weight);
  typedef typename std::vector<row, typename std::allocator_traits<A>::template rebind_alloc<row>> vector_row; // alias for users
  vector_row get_frequent_items(frequent_items_error_type err_type) const;
  vector_row get_frequent_items(frequent_items_error_type err_type, W threshold) const;
  size_t get_serialized_size_bytes() const;
  void serialize(std::ostream& os) const;
  typedef vector_u8<A> vector_bytes; // alias for users
  vector_bytes serialize(unsigned header_size_bytes = 0) const;
  static frequent_items_sketch deserialize(std::istream& is);
  static frequent_items_sketch deserialize(const void* bytes, size_t size);
  void to_stream(std::ostream& os, bool print_items = false) const;
private:
  static const uint8_t LG_MIN_MAP_SIZE = 3;
  static const uint8_t SERIAL_VERSION = 1;
  static const uint8_t FAMILY_ID = 10;
  static const uint8_t PREAMBLE_LONGS_EMPTY = 1;
  static const uint8_t PREAMBLE_LONGS_NONEMPTY = 4;
  static constexpr double EPSILON_FACTOR = 3.5;
  enum flags { IS_EMPTY };
  W total_weight;
  W offset;
  reverse_purge_hash_map<T, W, H, E, A> map;
  static void check_preamble_longs(uint8_t preamble_longs, bool is_empty);
  static void check_serial_version(uint8_t serial_version);
  static void check_family_id(uint8_t family_id);
  static void check_size(uint8_t lg_cur_size, uint8_t lg_max_size);

  // version for signed type
  template<typename WW = W, typename std::enable_if<std::is_signed<WW>::value, int>::type = 0>
  static inline void check_weight(WW weight);

  // version for unsigned type
  template<typename WW = W, typename std::enable_if<std::is_unsigned<WW>::value, int>::type = 0>
  static inline void check_weight(WW weight);
};

}

#include "frequent_items_sketch_impl.hpp"

# endif
