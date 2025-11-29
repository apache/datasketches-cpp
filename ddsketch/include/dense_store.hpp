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

#ifndef DENSE_STORE_HPP
#define DENSE_STORE_HPP

#include <vector>
#include "bin.hpp"
#include "common_defs.hpp"

namespace datasketches {

/**
 * @class DenseStore
 * @brief Contiguous integer-indexed bins backed by a growable array.
 *
 * @tparam Derived CRTP derived store.
 * @tparam Allocator Allocator type for internal storage.
 */
template<class Derived, typename Allocator>
class DenseStore {
public:

  /**
   * @brief Bin storage type (contiguous counts).
   */
  using bins_type = std::vector<double, typename std::allocator_traits<Allocator>::template rebind_alloc<double>>;

  /**
   * @brief Integer type for indices/lengths within this store.
   */
  using size_type = int;

  // Forward declarations
  /**
   * @brief Forward iterator over non-empty bins (ascending index)
   */
  class iterator;

  /**
   * @brief Reverse iterator over non-empty bins (descending index)
   */
  class reverse_iterator;

  /**
   * @brief Increment bin @p index by 1.
   */
  void add(int index);

  /**
   * @brief Increment bin @p index by @p count.
   */
  void add(int index, double count);

  /**
   * @brief Increment index by count as specified by @p bin.
   */
  void add(const Bin& bin);

  /**
   * @brief Clear all contents of the store.
   *
   * Removes all bins and resets counts to zero while preserving configuration
   * (e.g., capacity limits). After this call, @c total_count() is 0 and the
   * store contains no non-empty bins.
   */
  void clear();

  bool is_empty() const;

  /**
  * @brief Highest non-empty bin index.
  */
  size_type get_max_index() const;

  /**
  * @brief Lowest non-empty bin inde.
  */
  size_type get_min_index() const;

  /**
  * @brief Total count across all bins.
  */
  double get_total_count() const;

  /**
   * @brief Merge another dense store (same allocator) into this one.
   * @tparam Store Derived type of the other dense store.
   * @param other store; its counts are added here.
   */
  template<class Store>
  void merge(const DenseStore<Store, Allocator>& other);

  /**
   * This method serializes the store into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  /**
   * @brief Deserialize the store from a stream (replacing current contents).
   * @param is Input stream.
   */
  static Derived deserialize(std::istream& is);

  string<Allocator> to_string() const;

  bool operator==(const DenseStore<Derived, Allocator>& other) const;

  /**
   * @brief Begin iterator over non-empty bins (ascending).
   */
  iterator begin() const;

  /**
   * @brief End iterator over non-empty bins (ascending).
   */
  iterator end() const;

  /**
   * @brief Begin reverse iterator over non-empty bins (descending).
   */
  reverse_iterator rbegin() const;

  /**
   * @brief End reverse iterator over non-empty bins (descending).
   */
  reverse_iterator rend() const;

  ~DenseStore() = default;

  // ---------------- Iterators ----------------

  /**
   * @class DenseStore::iterator
   * @brief Input iterator yielding Bin values in ascending index order.
   *
   * Stable only while the store is not mutated.
   */
  class iterator {
  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Bin;
    using difference_type = std::ptrdiff_t;
    using pointer = Bin*;
    using reference = Bin;

    /**
     * @brief Construct positioned iterator (internal use).
     */
    iterator(const bins_type& bins, const size_type& index, const size_type& max_index, const size_type& offset);

    /**
     * @brief Assign from another iterator.
     */
    iterator& operator=(const iterator& other);


    /**
     * @brief Pre-increment.
     */
    iterator& operator++();

    /**
     * @brief Post-increment.
     */
    iterator operator++(int);

    /**
     * @brief Inequality comparison.
     */
    bool operator!=(const iterator& other) const;

    /**
     * @brief Dereference to the current Bin (index, count).
     */
    reference operator*() const;

  private:
    const bins_type& bins;
    size_type index;
    const size_type& max_index;
    const size_type& offset;
  };

  /**
   * @class DenseStore::reverse_iterator
   * @brief Input iterator yielding Bin values in descending index order.
   *
   * Stable only while the store is not mutated.
   */
  class reverse_iterator {
  public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Bin;
    using difference_type = std::ptrdiff_t;
    using pointer = Bin*;
    using reference = Bin;

    /**
     * @brief Construct positioned reverse iterator (internal use).
     */
    reverse_iterator(const bins_type& bins, size_type index, const size_type& min_index, const size_type& offset);

    /**
     * @brief Assign from another reverse iterator.
     */
    reverse_iterator& operator=(const reverse_iterator& other);

    /**
     * @brief Pre-increment.
     */
    reverse_iterator& operator++();

    /**
     * @brief Inequality comparison.
     */
    bool operator!=(const reverse_iterator& other) const;

    /**
     * @brief Dereference to the current Bin (index, count).
     */
    reference operator*() const;

  private:
    const bins_type& bins;
    size_type index;
    const size_type& min_index;
    const size_type& offset;
  };

protected:
  bins_type bins;
  size_type offset;
  size_type min_index;
  size_type max_index;

  const int array_length_growth_increment;
  const int array_length_overhead;

  static constexpr int DEFAULT_ARRAY_LENGTH_GROWTH_INCREMENT = 64;
  static constexpr double DEFAULT_ARRAY_LENGTH_OVERHEAD_RATIO = 0.1;

  // Protected constructors. This is a base class only and it is not meant to be instanced.
  DenseStore();
  explicit DenseStore(const int& array_length_growth_increment);
  explicit DenseStore(const int& array_length_growth_increment, const int& array_length_overhead);
  DenseStore(const DenseStore& other) = default;

  /**
   * @brief Total count in [@p from_index, @p to_index] (inclusive).
   */
  double get_total_count(size_type from_index, size_type to_index) const;

  /**
   * @brief Normalize a raw bin index into this store's current window.
   */
  size_type normalize(size_type index);

  /**
   * @brief Reframe the active index window to [new_min_index, new_max_index].
   */
  void adjust(size_type newMinIndex, size_type newMaxIndex);

  /**
   * @brief Extend window to include @p index (may grow and shift).
   */
  void extend_range(size_type index);

  /**
   * @brief Extend window to include [@p new_min_index, @p new_max_index].
   */
  void extend_range(size_type new_min_index, size_type new_max_index);

  /**
   * @brief Shift bins by @p shift (positive: toward higher indices).
   */
  void shift_bins(size_type shift);

  /**
   * @brief Center bins for the target window [@p new_min_index, @p new_max_index].
   */
  void center_bins(size_type new_min_index, size_type new_max_index);

  /**
   * @brief Compute the resized backing-array length for a target index span.
   *
   * @param new_min_index Lowest bin index to be retained (inclusive).
   * @param new_max_index Highest bin index to be retained (inclusive).
   * @return size_type New backing-array capacity (in bins).
   */
  size_type get_new_length(size_type new_min_index, size_type new_max_index) const;

  /**
   * @brief Zero all bins (keep capacity).
   */
  void reset_bins();

  /**
   * @brief Zero bins in [@p from_index, @p to_index] (inclusive).
   */
  void reset_bins(size_type from_index, size_type to_index);

  /**
   * @brief Serialize fields common to all dense stores.
   */
  void serialize_common(std::ostream& os) const;

  /**
   * @brief Derialize fields common to all dense stores.
   */
  static void deserialize_common(Derived& store, std::istream& is);

  /**
   * Computes size needed to serialize the current state of the sketch.
   * @return size in bytes needed to serialize this sketch
   */
  int get_serialized_size_bytes_common() const;

  Derived& derived();
  const Derived& derived() const;
};
}

#include "dense_store_impl.hpp"

#endif //DENSE_STORE_HPP
