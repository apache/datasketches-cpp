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

#ifndef _DDSKETCH_HPP_
#define _DDSKETCH_HPP_

#include "common_defs.hpp"
#include "memory_operations.hpp"

namespace datasketches {

/**
 * @class DDSketch
 * @brief A @c DDSketch with relative-error guarantees. This sketch computes quantile values
 * with an approximation error that is relative to the actual quantile value. It works with both
 * positive and negative input values.
*  DDSketch works by mapping floating-point input values to bins and counting the number
 * of values for each bin.
 *
 * @tparam Store underlying data structure that keeps track of bin counts.
 * @tparam Mapping maps an index to its corresponding bin.
 */
template<class Store, class Mapping>
class  DDSketch {
public:
  using vector_double = std::vector<double>;
  using T = typename Store::bins_type::value_type;

  /**
   * Constructs an initially empty quantile sketch using the specified {@link IndexMapping} and
   * {@link Store}.
   *
   * @param relative_accuracy sets the relative accuracy of the sketch.
   * For instance, using {@code DDSketch} with a relative accuracy guarantee set to 1%, if the
   * expected quantile value is 100, the computed quantile value is guaranteed to be between 99 and
   * 101.
   */
  explicit DDSketch(const double& relative_accuracy);

  /**
   * Constructs an initially empty quantile sketch using the specified {@link IndexMapping}.
   */
  explicit DDSketch(const Mapping& index_mapping);

  /**
   * Adds a value to the sketch.
   *
   * @param value the value to be added.
   * @param count the (optional) count to increase by. Default is 1.0.
   */
  void update(const double& value, const double& count = 1.0);

  /**
  * @brief Merge another {@link DDSketch} into this one.
  *
  * @param other DDSketch; its counts are added into this store.
  * @tparam OtherStore type of the other store.
  */
  template<class OtherStore>
  void merge(const DDSketch<OtherStore, Mapping>& other);

  /**
   * @brief Computes the rank of @p item in [0,1].
   * Defined as approximately (# of values ≤ @p item) / total_count, computed from
   * the sketch’s binned counts. Monotone in @p item and approximately the inverse
   * of @c get_quantile().
   */
  double get_rank(const double& item) const;

  /**
   * @brieg Computes the quantile k of @p item in [0,1].
   *
   * Returns a value v such that (approximately) @c get_rank(v) ≥ @p rank.
   */
  double get_quantile(const double& rank) const;

  vector_double get_PMF(const double* split_points, uint32_t size) const;

  vector_double get_CDF(const double* split_points, uint32_t size) const;


  bool is_empty() const;

  /**
   * @brief Clear all contents of the sketch.
   *
   * Calls clear() on the underlying stores.
   */
  void clear();

  /**
   * @return the total count hold by the sketch.
   */
  double get_count() const;

  /**
   * @return Sum of all inserted values.
   */
  double get_sum() const;
  /**
   * @return Min of all inserted values.
   */
  double get_min() const;
  /**
   * @return Max of all inserted values.
   */
  double get_max() const;

  /**
   * This method serializes the sketch into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  /**
   * @brief Deserialize the store from a stream (replacing current contents).
   * @param is Input stream.
   */
  static DDSketch deserialize(std::istream& is);


  /**
   * Computes size in bytes needed to serialize the current state of the sketch.
   * @return size in bytes needed to serialize this sketch
   */
  int get_serialized_size_bytes() const;

  template<class A = std::allocator<char>>
  string<A> to_string() const;

  bool operator==(const DDSketch<Store, Mapping>& other) const;
protected:

  /**
   * Protected constructor, meant to be used internally only.
   *
   * @param positive_store
   * @param negative_store
   * @param mapping
   * @param zero_count
   * @param min_indexed_value
   */
  DDSketch(const Store& positive_store, const Store& negative_store, const Mapping& mapping, const double& zero_count = 0.0, const double& min_indexed_value = 0.0);
  Store positive_store;
  Store negative_store;
  Mapping index_mapping;

  double zero_count;
  const double min_indexed_value;
  const double max_indexed_value;

  void check_value_trackable(const double& value) const;

  template<class OtherStore>
  void check_mergeability(const DDSketch<OtherStore, Mapping>& other) const;

  double get_quantile(const double& rank, const double& count) const;

  static inline void check_split_pints(const double* values, uint32_t size);
};

} /* namespace datasketches */

#include "ddsketch_impl.hpp"

#endif // _DDSKETCH_HPP_
