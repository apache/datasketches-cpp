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

#ifndef COLLAPSING_LOWEST_DENSE_STORE_HPP
#define COLLAPSING_LOWEST_DENSE_STORE_HPP

#include "collapsing_dense_store.hpp"

namespace datasketches {
template<int N, typename Allocator>
class CollapsingLowestDenseStore : public CollapsingDenseStore<CollapsingLowestDenseStore<N, Allocator>, N, Allocator> {
public:
  using size_type = typename CollapsingDenseStore<CollapsingLowestDenseStore, N, Allocator>::size_type;

  CollapsingLowestDenseStore();

  CollapsingLowestDenseStore* copy() const;

  void merge(const CollapsingLowestDenseStore& other);
  using DenseStore<CollapsingLowestDenseStore, Allocator>::merge;

protected:
  size_type normalize(size_type index);
  void adjust(size_type new_min_index, size_type new_max_index);

  friend class DenseStore<CollapsingLowestDenseStore, Allocator>;
};
}

#include "collapsing_lowest_dense_store_impl.hpp"

#endif //COLLAPSING_LOWEST_DENSE_STORE_HPP
