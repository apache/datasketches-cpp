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

#ifndef COLLAPSING_DENSE_STORE_IMPL_HPP
#define COLLAPSING_DENSE_STORE_IMPL_HPP

#include "collapsing_dense_store.hpp"

namespace datasketches {
template<typename Allocator>
CollapsingDenseStore<Allocator>::CollapsingDenseStore(size_type max_num_bins): DenseStore<Allocator>(), max_num_bins(max_num_bins), is_collapsed(false) {}

// TODO implement copy constructor

template<typename Allocator>
typename CollapsingDenseStore<Allocator>::size_type CollapsingDenseStore<Allocator>::get_new_length(size_type new_min_index, size_type new_max_index) const {
  return std::min(DenseStore<Allocator>::get_new_length(new_min_index, new_max_index), max_num_bins);
}

template<typename Allocator>
void CollapsingDenseStore<Allocator>::clear() {
  DenseStore<Allocator>::clear();
  is_collapsed = false;
}

}
#endif //COLLAPSING_DENSE_STORE_IMPL_HPP
