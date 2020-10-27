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

#include "serde.hpp"
#include "common_defs.hpp"

namespace datasketches {

// TODO: have a common random bit with KLL
static std::independent_bits_engine<std::mt19937, 1, uint8_t> req_random_bit(std::chrono::system_clock::now().time_since_epoch().count());

namespace req_constants {
  static const uint32_t MIN_K = 4;
  static const uint32_t INIT_NUM_SECTIONS = 3;
}

template<
typename T,
bool IsHighRank,
typename Comparator,
typename Allocator
>
class req_compactor {
public:
  req_compactor(uint8_t lg_weight, uint32_t section_size, const Allocator& allocator);

  bool is_sorted() const;
  uint32_t get_num_items() const;
  uint32_t get_nom_capacity() const;

  template<typename FwdT>
  void append(FwdT&& item);

  void sort();
  void merge_sort_in(std::vector<T, Allocator>&& items);

  std::vector<T, Allocator> compact();

private:
  uint8_t lg_weight_;
  bool coin_; // random bit for compaction
  bool sorted_;
  double section_size_raw_;
  uint32_t section_size_;
  uint32_t num_sections_;
  uint32_t num_compactions_;
  uint32_t state_; // state of the deterministic compaction schedule
  std::vector<T, Allocator> items_;

  bool ensure_enough_sections();
  size_t compute_compaction_range(uint32_t secs_to_compact) const;

  static uint32_t nearest_even(double value);

  template<typename Iter>
  static std::vector<T, Allocator> get_evens_or_odds(Iter from, Iter to, bool flag);
};

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

  req_sketch(uint32_t k, const Allocator& allocator = Allocator());

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

  /**
   * Prints a summary of the sketch.
   * @param print_levels if true include information about levels
   * @param print_items if true include sketch data
   */
  string<Allocator> to_string(bool print_levels = false, bool print_items = false) const;

private:
  Allocator allocator_;
  uint32_t k_;
  uint32_t max_nom_size_;
  uint32_t num_retained_;
  uint64_t n_;
  using AllocCompactor = typename std::allocator_traits<Allocator>::template rebind_alloc<Compactor>;
  std::vector<Compactor, AllocCompactor> compactors_;

  uint8_t get_num_levels() const;
  void grow();
  void update_max_nom_size();
  void update_num_retained();
  void compress();
};

} /* namespace datasketches */

#include "req_sketch_impl.hpp"

#endif
