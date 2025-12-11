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

#ifndef SPARSE_STORE_HPP
#define SPARSE_STORE_HPP

#include <map>
#include <string>

#include "bin.hpp"

/**
 * @class SparseStore
 * @brief Sparse integer-indexed bins container backed by a std::map.
 *
 * @tparam Allocator Allocator type for internal storage.
 */
namespace datasketches {
// Forward declaration
template<class Derived, typename Allocator> class DenseStore;

template<typename Allocator>
class SparseStore {
public:

  /**
   * @brief Bin storage type (contiguous counts).
   */
  using bins_type = std::map<
      int,
      double,
      std::less<int>,
      typename std::allocator_traits<Allocator>::template rebind_alloc<std::pair<const int, double>>
  >;

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
   * Default constructor
   */
  SparseStore() = default;

  bool operator==(const SparseStore &other) const;

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
   * @brief Create a heap-allocated copy of this store.
   * @return Pointer to a new CollapsingHighestDenseStore with identical contents.
   */
  SparseStore<Allocator>* copy() const;

  /**
   * @brief Clear all contents of the store.
   */
  void clear();

  /**
   * @brief Lowest non-empty bin inde.
   */
  int get_min_index() const;

  /**
   * @brief Highest non-empty bin index.
   */
  int get_max_index() const;

  /**
   * @brief Merge another sparse store (same allocator) into this one.
   * @param other store; its counts are added here.
   */
  void merge(const SparseStore<Allocator>& other);

  /**
   * @brief Merge a dense store (same allocator) into this one.
   * @tparam Derived type of the other dense store.
   * @param other store; its counts are added here.
   */
  template<class Derived>
  void merge(const DenseStore<Derived, Allocator>& other);


  bool is_empty() const;

  /**
   * @brief Total count across all bins.
   */
  double get_total_count() const;

  /**
   * This method serializes the store into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  /**
   * @brief Deserialize the store from a stream (replacing current contents).
   * @param is Input stream.
   */
  static SparseStore deserialize(std::istream& is);


  /**
   * Computes size needed to serialize the current state of the sketch.
   * @return size in bytes needed to serialize this sketch
   */
  int get_serialized_size_bytes() const;

  string<Allocator> to_string() const;
  /**
    * @brief Begin iterator over bins (ascending).
    */
  iterator begin() const;

  /**
   * @brief End iterator over bins (ascending).
   */
  iterator end() const;

  /**
   * @brief Begin reverse iterator over bins (descending).
   */
  reverse_iterator rbegin() const;

  /**
   * @brief End reverse iterator over bins (descending).
   */
  reverse_iterator rend() const;

  // ---------------- Iterators ----------------

  /**
   * @class SparseStore::iterator
   * @brief Input iterator yielding Bin values in ascending index order.
   *
   * Stable only while the store is not mutated.
   */
  class iterator {
  public:
    using internal_iterator = typename bins_type::const_iterator;
    using iterator_category = std::input_iterator_tag;
    using value_type = Bin;
    using difference_type = std::ptrdiff_t;
    using pointer = Bin*;
    using reference = Bin;

    /**
     * @brief Construct positioned iterator (internal use).
     */
    explicit iterator(internal_iterator it);

    /**
     * @brief Pre-increment.
     */
    iterator& operator++();

    /**
     * @brief Post-increment.
     */
    iterator operator++(int);

    /**
     * @brief Assign from another iterator.
     */
    iterator& operator=(const iterator& other);

    /**
     * @brief Inequality comparison.
     */
    bool operator!=(const iterator& other) const;

    /**
     * @brief Dereference to the current Bin (index, count).
     */
    reference operator*() const;

  private:
    internal_iterator it;
  };
  /**
     * @class SparseStore::reverse_iterator
     * @brief Input iterator yielding Bin values in descending index order.
     *
     * Stable only while the store is not mutated.
     */
  class reverse_iterator {
    public:
    using internal_iterator = typename bins_type::const_reverse_iterator;
    using iterator_category = std::input_iterator_tag;
    using value_type = Bin;
    using difference_type = std::ptrdiff_t;
    using pointer = Bin*;
    using reference = Bin;

    /**
     * @brief Construct positioned reverse iterator (internal use).
     */
    explicit reverse_iterator(internal_iterator it);

    /**
     * @brief Pre-increment.
     */
    reverse_iterator& operator++();

    /**
     * @brief Post-increment.
     */
    reverse_iterator operator++(int);

    /**
     * @brief Assign from another reverse iterator.
     */
    reverse_iterator& operator=(const reverse_iterator& other);

    /**
     * @brief Inequality comparison.
     */
    bool operator!=(const reverse_iterator& other) const;

    /**
     * @brief Dereference to the current Bin (index, count).
     */
    reference operator*() const;

  private:
    internal_iterator it;
  };


private:
  bins_type bins;
};
}

#include "sparse_store_impl.hpp"

#endif //SPARSE_STORE_HPP