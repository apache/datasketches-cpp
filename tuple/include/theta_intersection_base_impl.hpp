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

#include <iostream>
#include <sstream>
#include <algorithm>

namespace datasketches {

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
theta_intersection_base<EN, EK, P, S, CS, A>::theta_intersection_base(uint64_t seed, const P& policy):
policy_(policy),
is_valid_(false),
is_empty_(false),
lg_size_(0),
seed_hash_(compute_seed_hash(seed)),
num_entries_(0),
theta_(theta_constants::MAX_THETA),
entries_(nullptr)
{}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
theta_intersection_base<EN, EK, P, S, CS, A>::~theta_intersection_base() {
  destroy_objects();
  if (entries_ != nullptr) A().deallocate(entries_, 1 << lg_size_);
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
void theta_intersection_base<EN, EK, P, S, CS, A>::destroy_objects() {
  if (entries_ != nullptr) {
    const size_t size = 1 << lg_size_;
    for (size_t i = 0; i < size; ++i) {
      if (EK()(entries_[i]) != 0) {
        entries_[i].~EN();
        EK()(entries_[i]) = 0;
      }
    }
  }
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
void theta_intersection_base<EN, EK, P, S, CS, A>::update(const S& sketch) {
  if (is_empty_) return;
  if (!sketch.is_empty() && sketch.get_seed_hash() != seed_hash_) throw std::invalid_argument("seed hash mismatch");
  is_empty_ |= sketch.is_empty();
  theta_ = std::min(theta_, sketch.get_theta64());
  if (is_valid_ && num_entries_ == 0) return;
  if (sketch.get_num_retained() == 0) {
    is_valid_ = true;
    destroy_objects();
    A().deallocate(entries_, 1 << lg_size_);
    entries_ = nullptr;
    lg_size_ = 0;
    num_entries_ = 0;
    return;
  }
  if (!is_valid_) { // first update, clone incoming sketch
    is_valid_ = true;
    lg_size_ = lg_size_from_count(sketch.get_num_retained(), theta_update_sketch_base<EN, EK, A>::REBUILD_THRESHOLD);
    const size_t size = 1 << lg_size_;
    entries_ = A().allocate(size);
    for (size_t i = 0; i < size; ++i) EK()(entries_[i]) = 0;
    for (const auto& entry: sketch) {
      auto result = theta_update_sketch_base<EN, EK, A>::find(entries_, lg_size_, EK()(entry));
      if (result.second) {
        throw std::invalid_argument("duplicate key, possibly corrupted input sketch");
      }
      new (result.first) EN(entry); // TODO: support move
      ++num_entries_;
    }
    if (num_entries_ != sketch.get_num_retained()) throw std::invalid_argument("num entries mismatch, possibly corrupted input sketch");
  } else { // intersection
    const uint32_t max_matches = std::min(num_entries_, sketch.get_num_retained());
    std::vector<EN, A> matched_entries;
    matched_entries.reserve(max_matches);
    uint32_t match_count = 0;
    uint32_t count = 0;
    for (const auto& entry: sketch) {
      if (EK()(entry) < theta_) {
        auto result = theta_update_sketch_base<EN, EK, A>::find(entries_, lg_size_, EK()(entry));
        if (result.second) {
          if (match_count == max_matches) throw std::invalid_argument("max matches exceeded, possibly corrupted input sketch");
          matched_entries.push_back(policy_(*result.first, entry));
          ++match_count;
        }
      } else if (sketch.is_ordered()) {
        break; // early stop
      }
      ++count;
    }
    std::cout << "intersection: matching done" << std::endl;
    if (count > sketch.get_num_retained()) {
      throw std::invalid_argument(" more keys then expected, possibly corrupted input sketch");
    } else if (!sketch.is_ordered() && count < sketch.get_num_retained()) {
      throw std::invalid_argument(" fewer keys then expected, possibly corrupted input sketch");
    }
    destroy_objects();
    if (match_count == 0) {
      A().deallocate(entries_, 1 << lg_size_);
      entries_ = nullptr;
      lg_size_ = 0;
      num_entries_ = 0;
      if (theta_ == theta_constants::MAX_THETA) is_empty_ = true;
    } else {
      std::cout << "intersection: converting to hash map" << std::endl;
      const uint8_t lg_size = lg_size_from_count(match_count, theta_update_sketch_base<EN, EK, A>::REBUILD_THRESHOLD);
      const size_t size = 1 << lg_size;
      if (lg_size != lg_size_) {
        std::cout << "intersection: resizing from " << (1<<lg_size_) << " to " << size << std::endl;
        A().deallocate(entries_, 1 << lg_size_);
        entries_ = nullptr;
        lg_size_ = lg_size;
        entries_ = A().allocate(size);
        for (size_t i = 0; i < size; ++i) EK()(entries_[i]) = 0;
      }
      for (uint32_t i = 0; i < match_count; i++) {
        auto result = theta_update_sketch_base<EN, EK, A>::find(entries_, lg_size_, EK()(matched_entries[i]));
        new (result.first) EN(std::move(matched_entries[i]));
      }
      num_entries_ = match_count;
    }
  }
}

template<typename EN, typename EK, typename P, typename S, typename CS, typename A>
CS theta_intersection_base<EN, EK, P, S, CS, A>::get_result(bool ordered) const {
  std::vector<EN, A> entries_copy;
  if (num_entries_ > 0) {
    entries_copy.reserve(num_entries_);
    std::copy_if(&entries_[0], &entries_[1 << lg_size_], std::back_inserter(entries_copy), key_not_zero<EN, EK>());
    if (ordered) std::sort(entries_copy.begin(), entries_copy.end(), comparator());
  }
  return CS(is_empty_, ordered, seed_hash_, theta_, std::move(entries_copy));
}

} /* namespace datasketches */
