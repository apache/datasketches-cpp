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

#ifndef QUANTILE_SKETCH_SORTED_VIEW_IMPL_HPP_
#define QUANTILE_SKETCH_SORTED_VIEW_IMPL_HPP_

#include <algorithm>

namespace datasketches {

template<typename T, typename C, typename A>
quantile_sketch_sorted_view<T, C, A>::quantile_sketch_sorted_view(const A& allocator):
total_weight_(0),
entries_(allocator)
{}

template<typename T, typename C, typename A>
template<typename Iterator>
void quantile_sketch_sorted_view<T, C, A>::add(Iterator first, Iterator last, uint64_t weight) {
  if (entries_.capacity() < entries_.size() + std::distance(first, last)) entries_.reserve(entries_.size() + std::distance(first, last));
  const size_t size_before = entries_.size();
  for (auto it = first; it != last; ++it) entries_.push_back(Entry(&*it, weight));
  if (size_before > 0) std::inplace_merge(entries_.begin(), entries_.begin() + size_before, entries_.end(), compare_pairs_by_first_ptr<C>());
}

template<typename T, typename C, typename A>
template<bool inclusive>
void quantile_sketch_sorted_view<T, C, A>::convert_to_cummulative() {
  uint64_t subtotal = 0;
  for (auto& entry: entries_) {
    const uint64_t new_subtotal = subtotal + entry.second;
    entry.second = inclusive ? new_subtotal : subtotal;
    subtotal = new_subtotal;
  }
  total_weight_ = subtotal;
}

template<typename T, typename C, typename A>
const T& quantile_sketch_sorted_view<T, C, A>::get_quantile(double rank) const {
  if (total_weight_ == 0) throw std::invalid_argument("supported for cumulative weight only");
  uint64_t weight = static_cast<uint64_t>(rank * total_weight_);
  auto it = std::lower_bound(entries_.begin(), entries_.end(), Entry(nullptr, weight), compare_pairs_by_second());
  if (it == entries_.end()) return *(entries_[entries_.size() - 1].first);
  return *(it->first);
}

template<typename T, typename C, typename A>
auto quantile_sketch_sorted_view<T, C, A>::begin() const -> const_iterator {
  return entries_.begin();
}

template<typename T, typename C, typename A>
auto quantile_sketch_sorted_view<T, C, A>::end() const -> const_iterator {
  return entries_.end();
}

template<typename T, typename C, typename A>
size_t quantile_sketch_sorted_view<T, C, A>::size() const {
  return entries_.size();
}

} /* namespace datasketches */

#endif
