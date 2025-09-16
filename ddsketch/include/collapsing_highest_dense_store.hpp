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

#ifndef COLLAPSING_HIGHEST_DENSE_STORE_HPP
#define COLLAPSING_HIGHEST_DENSE_STORE_HPP

#include "collapsing_dense_store.hpp"

namespace datasketches {
/**
 * @class CollapsingHighestDenseStore
 * @brief Capacity-bounded dense store collapsing from the higher end.
 * @tparam N Maximum number of bins (capacity limit).
 * @tparam Allocator Allocator type for internal storage.
 *
 * When capacity is exceeded, the highest-index bins are merged into one,
 * preserving total count while reducing resolution in the high tail.
 */
template<int N, typename Allocator>
class CollapsingHighestDenseStore : public CollapsingDenseStore<CollapsingHighestDenseStore<N, Allocator>, N, Allocator> {
public:
  using size_type = typename CollapsingDenseStore<CollapsingHighestDenseStore, N, Allocator>::size_type;

  /**
   * @brief Constructor.
   */
  CollapsingHighestDenseStore();

  /**
   * @brief Create a heap-allocated copy of this store.
   * @return Pointer to a new CollapsingHighestDenseStore with identical contents.
   */
  CollapsingHighestDenseStore* copy() const;

  /**
   * @brief Merge another store into this one.
   * @param other Source store; its counts are added into this store.
   * @note May trigger tail collapsing to respect the capacity @tparam N.
   */
  void merge(const CollapsingHighestDenseStore& other);

  /**
   * @brief Bring base-class merge overloads into scope (e.g., generic Store/DenseStore merges).
   */
  using DenseStore<CollapsingHighestDenseStore, Allocator>::merge;

protected:

  /**
   * @brief Normalize a raw bin index into this store's current window.
   *
   * If @p index exceeds the current @c max_index, the range is extended; if
   * extension causes the store to collapse the high tail, the normalized index
   * is the last bin. If @p index is below @c min_index, the range is extended
   * on the low side. Otherwise returns the in-range offset.
   *
   * @param index Raw (possibly out-of-range) bin index.
   * @return size_type In-range index (offset from @c offset) used for storage.
   */
  size_type normalize(size_type index);


  void adjust(size_type new_min_index, size_type new_max_index);

  friend class DenseStore<CollapsingHighestDenseStore, Allocator>;
};
}

#include "collapsing_highest_dense_store_impl.hpp"
#endif //COLLAPSING_HIGHEST_DENSE_STORE_HPP