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

#ifndef DENSITY_SKETCH_HPP_
#define DENSITY_SKETCH_HPP_

#include <type_traits>
#include <vector>
#include <functional>
#include <numeric>
#include <cmath>

#include "common_defs.hpp"

/*
 * Based on the following paper:
 * Zohar Karnin, Edo Liberty "Discrepancy, Coresets, and Sketches in Machine Learning"
 * https://proceedings.mlr.press/v99/karnin19a/karnin19a.pdf
 */

namespace datasketches {

template<typename T>
struct gaussian_kernel {
  T operator()(const std::vector<T>& v1, const std::vector<T>& v2) const {
    return exp(-std::inner_product(v1.begin(), v1.end(), v2.begin(), 0, std::plus<T>(), [](T a, T b){return (a-b)*(a-b);}));
  }
};

template<
  typename T,
  typename Kernel = gaussian_kernel<T>,
  typename Allocator = std::allocator<T>
>
class density_sketch {
  static_assert(std::is_floating_point<T>::value, "Floating point type expected");

public:
  using Vector = std::vector<T, Allocator>;
  using Level = std::vector<Vector, typename std::allocator_traits<Allocator>::template rebind_alloc<Vector>>;
  using Levels = std::vector<Level, typename std::allocator_traits<Allocator>::template rebind_alloc<Level>>;

  /**
   * Constructor
   * @param k controls the size and error of the sketch.
   * @param dim dimension of the input domain
   * @param allocator to use by this instance
   */
  density_sketch(uint16_t k, uint32_t dim, const Allocator& allocator = Allocator());

  /**
   * Returns configured parameter K
   * @return parameter K
   */
  uint16_t get_k() const;

  /**
   * Returns configured dimensions
   * @return dimensions
   */
  uint32_t get_dim() const;

  /**
   * Returns true if this sketch is empty.
   * @return empty flag
   */
  bool is_empty() const;

  /**
   * Returns the length of the input stream (number of points observed by this sketch).
   * @return stream length
   */
  uint64_t get_n() const;

  /**
   * Returns the number of retained points in the sketch.
   * @return number of retained points
   */
  uint32_t get_num_retained() const;

  /**
   * Returns true if this sketch is in estimation mode.
   * @return estimation mode flag
   */
  bool is_estimation_mode() const;

  /**
   * Updates this sketch with a given point.
   * @param point given point
   */
  template<typename FwdVector>
  void update(FwdVector&& point);

  /**
   * Merges another sketch into this one.
   * @param other sketch to merge into this one
   */
  template<typename FwdSketch>
  void merge(FwdSketch&& other);

  T get_estimate(const std::vector<T>& point) const;

  /**
   * Returns an instance of the allocator for this sketch.
   * @return allocator
   */
  Allocator get_allocator() const;

  /**
   * Prints a summary of the sketch.
   * @param print_levels if true include information about levels
   * @param print_items if true include sketch data
   */
  string<Allocator> to_string(bool print_levels = false, bool print_items = false) const;

  class const_iterator;
  const_iterator begin() const;
  const_iterator end() const;

private:
  uint16_t k_;
  uint32_t dim_;
  uint32_t num_retained_;
  uint64_t n_;
  Levels levels_;

  void compact();
  void compact_level(unsigned height);
};

template<typename T, typename K, typename A>
class density_sketch<T, K, A>::const_iterator: public std::iterator<std::input_iterator_tag, T> {
public:
  using Vector = density_sketch<T, K, A>::Vector;
  using value_type = std::pair<const Vector&, const uint64_t>;
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  const value_type operator*() const;
  const return_value_holder<value_type> operator->() const;
private:
  using LevelsIterator = typename density_sketch<T, K, A>::Levels::const_iterator;
  using LevelIterator = typename density_sketch<T, K, A>::Level::const_iterator;
  LevelsIterator levels_it_;
  LevelsIterator levels_end_;
  LevelIterator level_it_;
  unsigned height_;
  friend class density_sketch<T, K, A>;
  const_iterator(LevelsIterator begin, LevelsIterator end);
};

} /* namespace datasketches */

#include "density_sketch_impl.hpp"

#endif
