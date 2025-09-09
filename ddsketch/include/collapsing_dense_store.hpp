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

template<class Derived, typename Allocator>
class CollapsingDenseStore : public DenseStore<Derived, Allocator> {
public:

  using size_type = typename DenseStore<Derived, Allocator>::size_type;
  explicit CollapsingDenseStore(size_type max_num_bins);
  CollapsingDenseStore(const CollapsingDenseStore<Derived, Allocator>& other) = default;

  ~CollapsingDenseStore() = default;

  void clear();

protected:
  const size_type max_num_bins;
  bool is_collapsed;

  size_type get_new_length(size_type new_min_index, size_type new_max_index) const;
};
}

#include "collapsing_dense_store_impl.hpp"

#endif //COLLAPSING_DENSE_STORE_HPP
