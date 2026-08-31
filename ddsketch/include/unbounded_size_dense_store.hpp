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

#ifndef UNBOUNDED_SIZE_DENSE_STORE_HPP
#define UNBOUNDED_SIZE_DENSE_STORE_HPP
#include "dense_store.hpp"

namespace datasketches {

/**
 * @class UnboundedSizeDenseStore
 * @brief Common logic for non-bounded-capacity dense stores.
 */
template<typename Allocator>
class UnboundedSizeDenseStore: public DenseStore<UnboundedSizeDenseStore<Allocator>, Allocator> {
public:
  using size_type = typename DenseStore<UnboundedSizeDenseStore, Allocator>::size_type;

  // Constructors
  UnboundedSizeDenseStore();
  explicit UnboundedSizeDenseStore(const int& array_length_growth_increment);
  explicit UnboundedSizeDenseStore(const int& array_length_growth_increment, const int& array_length_overhead);

  /**
   * @brief Create a heap-allocated copy of this store.
   * @return Pointer to a new   UnboundedSizeDenseStore* copy() const;
   */
  UnboundedSizeDenseStore* copy() const;

  ~UnboundedSizeDenseStore() = default;

  /**
   * Copy assignment
   * @param other sketch to be copied
   * @return reference to this sketch
   */
  UnboundedSizeDenseStore& operator=(const UnboundedSizeDenseStore& other);

  /**
   * @brief Merge another store into this one.
   * @param other Source store; its counts are added into this store.
   * @note May trigger tail collapsing to respect the capacity @tparam N.
   */
  void merge(const UnboundedSizeDenseStore<Allocator>& other);

  /**
   * @brief Bring base-class merge overloads into scope (e.g., generic Store/DenseStore merges).
   */
  using DenseStore<UnboundedSizeDenseStore, Allocator>::merge;

  /**
   * This method serializes the store into a given stream in a binary form
   * @param os output stream
   */
  void serialize(std::ostream& os) const;

  /**
   * @brief Deserialize the store from a stream (replacing current contents).
   * @param is Input stream.
   */
  static UnboundedSizeDenseStore deserialize(std::istream& is);

  /**
   * Computes size needed to serialize the current state of the sketch.
   * @return size in bytes needed to serialize this sketch
   */
  int get_serialized_size_bytes() const;

protected:

  /**
   * @brief Normalize a raw bin index into this store's current window.
   */
  size_type normalize(size_type index);

  /**
   * @brief Reframe the active index window to [new_min_index, new_max_index].
   */
  void adjust(size_type new_min_index, size_type new_max_index);

  friend class DenseStore<UnboundedSizeDenseStore, Allocator>;
};
}

#include "unbounded_size_dense_store_impl.hpp"

#endif //UNBOUNDED_SIZE_DENSE_STORE_HPP
