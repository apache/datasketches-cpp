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

#ifndef REQ_QUANTILE_CALCULATOR_IMPL_HPP_
#define REQ_QUANTILE_CALCULATOR_IMPL_HPP_

namespace datasketches {

template<typename T, typename A>
req_quantile_calculator<T, A>::req_quantile_calculator(uint64_t n, const A& allocator):
n_(n),
entries_(allocator)
{}

template<typename T, typename A>
void req_quantile_calculator<T, A>::add(const std::vector<T, A>& items, uint8_t lg_weight) {
  if (entries_.capacity() < entries_.size() + items.size()) entries_.reserve(entries_.size() + items.size());
  auto middle = entries_.end();
  for (const auto& item: items) entries_.push_back(Entry(&item, 1 << lg_weight));
  std::inplace_merge(entries_.begin(), middle, entries_.end(), compare_pairs_by_second());
}

template<typename T, typename A>
template<bool inclusive>
void req_quantile_calculator<T, A>::convert_to_cummulative() {
  uint64_t subtotal = 0;
  for (auto& entry: entries_) {
    const uint64_t new_subtotal = subtotal + entry.second;
    entry.second = inclusive ? new_subtotal : subtotal;
    subtotal = new_subtotal;
  }
}

template<typename T, typename A>
const T& req_quantile_calculator<T, A>::get_quantile(double rank) const {
  uint64_t weight = rank * n_;
  auto it = std::lower_bound(entries_.begin(), entries_.end(), Entry(nullptr, weight), compare_pairs_by_second());
  if (it == entries_.end()) return *(entries_[entries_.size() - 1].first);
  return *(it->first);
}

} /* namespace datasketches */

#endif
