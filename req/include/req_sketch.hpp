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
