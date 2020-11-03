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

#ifndef REQ_SKETCH_IMPL_HPP_
#define REQ_SKETCH_IMPL_HPP_

#include <sstream>

namespace datasketches {

template<typename T, bool H, typename C, typename S, typename A>
req_sketch<T, H, C, S, A>::req_sketch(uint32_t k, const A& allocator):
allocator_(allocator),
k_(k),
max_nom_size_(0),
num_retained_(0),
n_(0)
{
  grow();
}

template<typename T, bool H, typename C, typename S, typename A>
bool req_sketch<T, H, C, S, A>::is_empty() const {
  return n_ == 0;
}

template<typename T, bool H, typename C, typename S, typename A>
uint64_t req_sketch<T, H, C, S, A>::get_n() const {
  return n_;
}

template<typename T, bool H, typename C, typename S, typename A>
uint32_t req_sketch<T, H, C, S, A>::get_num_retained() const {
  return num_retained_;
}

template<typename T, bool H, typename C, typename S, typename A>
bool req_sketch<T, H, C, S, A>::is_estimation_mode() const {
  return compactors_.size() > 1;
}

template<typename T, bool H, typename C, typename S, typename A>
template<typename FwdT>
void req_sketch<T, H, C, S, A>::update(FwdT&& item) {
//  if (Float.isNaN(item)) { return; }
//  if (isEmpty()) {
//    minValue = item;
//    maxValue = item;
//  } else {
//    if (item < minValue) { minValue = item; }
//    if (item > maxValue) { maxValue = item; }
//  }
  compactors_[0].append(item);
  ++num_retained_;
  ++n_;
  if (num_retained_ == max_nom_size_) {
    compactors_[0].sort();
    compress();
  }
  //  aux = null;
}


template<typename T, bool H, typename C, typename S, typename A>
template<bool inclusive>
double req_sketch<T, H, C, S, A>::get_rank(const T& item) const {
  uint64_t weight = 0;
  for (const auto& compactor: compactors_) {
    weight += compactor.template compute_weight<inclusive>(item);
  }
  return static_cast<double>(weight) / n_;
}

template<typename T, bool H, typename C, typename S, typename A>
template<bool inclusive>
const T& req_sketch<T, H, C, S, A>::get_quantile(double rank) const {
  if (is_empty()) throw new std::invalid_argument("sketch is empty");
  if ((rank < 0.0) || (rank > 1.0)) {
    throw std::invalid_argument("Rank cannot be less than zero or greater than 1.0");
  }
  // TODO: min and max
  if (!compactors_[0].is_sorted()) {
    const_cast<req_compactor<T, H, C, A>&>(compactors_[0]).sort(); // allow this side effect
  }
  req_quantile_calculator<T, A> quantile_calculator(n_, allocator_);
  for (auto& compactor: compactors_) {
    quantile_calculator.add(compactor.get_items(), compactor.get_lg_weight());
  }
  quantile_calculator.template convert_to_cummulative<inclusive>();
  return quantile_calculator.get_quantile(rank);
}

template<typename T, bool H, typename C, typename S, typename A>
void req_sketch<T, H, C, S, A>::grow() {
  const uint8_t lg_weight = get_num_levels();
  compactors_.push_back(Compactor(lg_weight, k_, allocator_));
  update_max_nom_size();
}

template<typename T, bool H, typename C, typename S, typename A>
uint8_t req_sketch<T, H, C, S, A>::get_num_levels() const {
  return compactors_.size();
}

template<typename T, bool H, typename C, typename S, typename A>
void req_sketch<T, H, C, S, A>::update_max_nom_size() {
  max_nom_size_ = 0;
  for (const auto& compactor: compactors_) max_nom_size_ += compactor.get_nom_capacity();
}

template<typename T, bool H, typename C, typename S, typename A>
void req_sketch<T, H, C, S, A>::update_num_retained() {
  num_retained_ = 0;
  for (const auto& compactor: compactors_) num_retained_ += compactor.get_num_items();
}

template<typename T, bool H, typename C, typename S, typename A>
void req_sketch<T, H, C, S, A>::compress() {
  for (size_t h = 0; h < compactors_.size(); ++h) {
    if (compactors_[h].get_num_items() >= compactors_[h].get_nom_capacity()) {
      if (h + 1 >= get_num_levels()) { // at the top?
        grow(); // add a level, increases max_nom_size
      }
      auto promoted = compactors_[h].compact();
      compactors_[h + 1].merge_sort_in(std::move(promoted));
      update_num_retained();
      if (num_retained_ < max_nom_size_) break;
    }
  }
  update_max_nom_size();
  // aux = null;
}

template<typename T, bool H, typename C, typename S, typename A>
string<A> req_sketch<T, H, C, S, A>::to_string(bool print_levels, bool print_items) const {
  std::basic_ostringstream<char, std::char_traits<char>, AllocChar<A>> os;
  os << "### REQ sketch summary:" << std::endl;
  os << "   K              : " << k_ << std::endl;
  os << "   High Rank Acc  : " << (H ? "true" : "false") << std::endl;
  os << "   Empty          : " << (is_empty() ? "true" : "false") << std::endl;
  os << "   Estimation mode: " << (is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   Sorted         : " << (compactors_[0].is_sorted() ? "true" : "false") << std::endl;
  os << "   N              : " << n_ << std::endl;
  os << "   Levels         : " << compactors_.size() << std::endl;
  os << "   Retained items : " << num_retained_ << std::endl;
  os << "   Capacity items : " << max_nom_size_ << std::endl;
//  os << "   Storage bytes  : " << get_serialized_size_bytes() << std::endl;
//  if (!is_empty()) {
//    os << "   Min value      : " << *min_value_ << std::endl;
//    os << "   Max value      : " << *max_value_ << std::endl;
//  }
  os << "### End sketch summary" << std::endl;

  if (print_levels) {
    os << "### REQ sketch levels:" << std::endl;
    os << "   index: nominal capacity, actual size" << std::endl;
    for (uint8_t i = 0; i < compactors_.size(); i++) {
      os << "   " << (unsigned int) i << ": "
        << compactors_[i].get_nom_capacity() << ", "
        << compactors_[i].get_num_items() << std::endl;
    }
    os << "### End sketch levels" << std::endl;
  }

  if (print_items) {
    os << "### REQ sketch data:" << std::endl;
    unsigned level = 0;
    for (const auto& compactor: compactors_) {
      os << " level " << level << ": " << std::endl;
      for (const auto& item: compactor.get_items()) {
        os << "   " << item << std::endl;
      }
      ++level;
    }
    os << "### End sketch data" << std::endl;
  }
  return os.str();
}

} /* namespace datasketches */

#endif
