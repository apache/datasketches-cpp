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

#ifndef COLLAPSING_DENSE_STORE_HPP
#define COLLAPSING_DENSE_STORE_HPP

#include "dense_store.hpp"

namespace datasketches {

/**
 * @class CollapsingDenseStore
 * @brief Common logic for capacity-bounded dense stores with tail-collapsing.
 */
template<class Derived, int N, typename Allocator>
class CollapsingDenseStore : public DenseStore<Derived, Allocator> {
public:

  using size_type = typename DenseStore<Derived, Allocator>::size_type;
  CollapsingDenseStore();

  /**
   * Copy assignment
   * @param other sketch to be copied
   * @return reference to this sketch
   */
  CollapsingDenseStore<Derived, N, Allocator>& operator=(const CollapsingDenseStore<Derived, N, Allocator>& other);

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

  /**
   * Computes size needed to serialize the current state of the sketch.
   * @return size in bytes needed to serialize this sketch
   */
  int get_serialized_size_bytes() const;

  ~CollapsingDenseStore() = default;

  /**
   * @brief Clear all contents of the store.
   *
   * Removes all bins and resets counts to zero while preserving configuration
   * (e.g., capacity limits). After this call, @c total_count() is 0 and the
   * store contains no non-empty bins.
   */
  void clear();

protected:
  bool is_collapsed;

  /**
   * @brief Compute the resized backing-array length for a target index span.
   *
   * @param new_min_index Lowest bin index to be retained (inclusive).
   * @param new_max_index Highest bin index to be retained (inclusive).
   * @return size_type New backing-array capacity (in bins).
   */
  size_type get_new_length(size_type new_min_index, size_type new_max_index) const;
};
}

#include "collapsing_dense_store_impl.hpp"

#endif //COLLAPSING_DENSE_STORE_HPP