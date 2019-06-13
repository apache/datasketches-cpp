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

#ifndef THETA_INTERSECTION_IMPL_HPP_
#define THETA_INTERSECTION_IMPL_HPP_

#include <algorithm>

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
theta_intersection_alloc<A>::theta_intersection_alloc(uint64_t seed):
is_valid_(false),
is_empty_(false),
theta_(theta_sketch_alloc<A>::MAX_THETA),
lg_size_(0),
keys_(nullptr),
num_keys_(0),
seed_hash_(theta_sketch_alloc<A>::get_seed_hash(seed))
{}

template<typename A>
theta_intersection_alloc<A>::theta_intersection_alloc(const theta_intersection_alloc<A>& other):
is_valid_(other.is_valid_),
is_empty_(other.is_empty_),
theta_(other.theta_),
lg_size_(other.lg_size_),
keys_(other.keys_ == nullptr ? nullptr : AllocU64().allocate(1 << lg_size_)),
num_keys_(other.num_keys_),
seed_hash_(other.seed_hash_)
{
  if (keys_ != nullptr) std::copy(other.keys_, &other.keys_[1 << lg_size_], keys_);
}

template<typename A>
theta_intersection_alloc<A>::theta_intersection_alloc(theta_intersection_alloc<A>&& other) noexcept:
is_valid_(false),
is_empty_(false),
theta_(theta_sketch_alloc<A>::MAX_THETA),
lg_size_(0),
keys_(nullptr),
num_keys_(0),
seed_hash_(other.seed_hash_)
{
  std::swap(is_valid_, other.is_valid_);
  std::swap(is_empty_, other.is_empty_);
  std::swap(theta_, other.theta_);
  std::swap(lg_size_, other.lg_size_);
  std::swap(keys_, other.keys_);
  std::swap(num_keys_, other.num_keys_);
}

template<typename A>
theta_intersection_alloc<A>::~theta_intersection_alloc() {
  if (keys_ != nullptr) {
    AllocU64().deallocate(keys_, 1 << lg_size_);
  }
}

template<typename A>
theta_intersection_alloc<A>& theta_intersection_alloc<A>::operator=(theta_intersection_alloc<A> other) {
  std::swap(is_valid_, other.is_valid_);
  std::swap(is_empty_, other.is_empty_);
  std::swap(theta_, other.theta_);
  std::swap(lg_size_, other.lg_size_);
  std::swap(keys_, other.keys_);
  std::swap(num_keys_, other.num_keys_);
  std::swap(seed_hash_, other.seed_hash_);
  return *this;
}

template<typename A>
theta_intersection_alloc<A>& theta_intersection_alloc<A>::operator=(theta_intersection_alloc<A>&& other) {
  std::swap(is_valid_, other.is_valid_);
  std::swap(is_empty_, other.is_empty_);
  std::swap(theta_, other.theta_);
  std::swap(lg_size_, other.lg_size_);
  std::swap(keys_, other.keys_);
  std::swap(num_keys_, other.num_keys_);
  std::swap(seed_hash_, other.seed_hash_);
  return *this;
}

template<typename A>
void theta_intersection_alloc<A>::update(const theta_sketch_alloc<A>& sketch) {
  if (is_empty_) return;
  if (sketch.get_seed_hash() != seed_hash_) throw std::invalid_argument("seed hash mismatch");
  is_empty_ |= sketch.is_empty();
  theta_ = std::min(theta_, sketch.get_theta64());
  if (is_valid_ and num_keys_ == 0) return;
  if (sketch.get_num_retained() == 0) {
    is_valid_ = true;
    if (keys_ != nullptr) {
      AllocU64().deallocate(keys_, 1 << lg_size_);
      keys_ = nullptr;
      lg_size_ = 0;
      num_keys_ = 0;
    }
    return;
  }
  if (!is_valid_) { // first update, clone incoming sketch
    is_valid_ = true;
    lg_size_ = lg_size_from_count(sketch.get_num_retained(), update_theta_sketch_alloc<A>::REBUILD_THRESHOLD);
    keys_ = AllocU64().allocate(1 << lg_size_);
    std::fill(keys_, &keys_[1 << lg_size_], 0);
    num_keys_ = sketch.get_num_retained();
    for (auto key: sketch) update_theta_sketch_alloc<A>::hash_search_or_insert(key, keys_, lg_size_);
  } else { // intersection
    const uint32_t max_matches = std::min(num_keys_, sketch.get_num_retained());
    uint64_t* matched_keys = AllocU64().allocate(max_matches);
    uint32_t match_count = 0;
    for (auto key: sketch) {
      if (key < theta_) {
        if (update_theta_sketch_alloc<A>::hash_search(key, keys_, lg_size_)) matched_keys[match_count++] = key;
      } else if (sketch.is_ordered()) {
        break; // early stop
      }
    }
    if (match_count == 0) {
      AllocU64().deallocate(keys_, 1 << lg_size_);
      keys_ = nullptr;
      lg_size_ = 0;
      num_keys_ = 0;
      if (theta_ == theta_sketch_alloc<A>::MAX_THETA) is_empty_ = true;
    } else {
      const uint8_t lg_size = lg_size_from_count(match_count, update_theta_sketch_alloc<A>::REBUILD_THRESHOLD);
      if (lg_size != lg_size_) {
        AllocU64().deallocate(keys_, 1 << lg_size_);
        lg_size_ = lg_size;
        keys_ = AllocU64().allocate(1 << lg_size_);
        std::fill(keys_, &keys_[1 << lg_size_], 0);
      }
      for (uint32_t i = 0; i < match_count; i++) {
        update_theta_sketch_alloc<A>::hash_search_or_insert(matched_keys[i], keys_, lg_size_);
      }
      num_keys_ = match_count;
    }
    AllocU64().deallocate(matched_keys, max_matches);
  }
}

template<typename A>
compact_theta_sketch_alloc<A> theta_intersection_alloc<A>::get_result(bool ordered) const {
  if (!is_valid_) throw std::invalid_argument("calling get_result() before calling update() is undefined");
  if (num_keys_ == 0) return compact_theta_sketch_alloc<A>(is_empty_, theta_, nullptr, 0, seed_hash_, ordered);
  uint64_t* keys = AllocU64().allocate(num_keys_);
  std::copy_if(keys_, &keys_[1 << lg_size_], keys, [](uint64_t key) { return key != 0; });
  if (ordered) std::sort(keys, &keys[num_keys_]);
  return compact_theta_sketch_alloc<A>(false, this->theta_, keys, num_keys_, seed_hash_, ordered);
}

template<typename A>
bool theta_intersection_alloc<A>::has_result() const {
  return is_valid_;
}

} /* namespace datasketches */

# endif
