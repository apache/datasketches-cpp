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

#ifndef THETA_A_NOT_B_IMPL_HPP_
#define THETA_A_NOT_B_IMPL_HPP_

#include <algorithm>

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
theta_a_not_b_alloc<A>::theta_a_not_b_alloc(uint64_t seed):
seed_hash_(theta_sketch_alloc<A>::get_seed_hash(seed))
{}

template<typename A>
compact_theta_sketch_alloc<A> theta_a_not_b_alloc<A>::compute(const theta_sketch_alloc<A>& a, const theta_sketch_alloc<A>& b, bool ordered) const {
  if (a.is_empty()) return compact_theta_sketch_alloc<A>(a, ordered);
  if (a.get_seed_hash() != seed_hash_) throw std::invalid_argument("A seed hash mismatch");
  if (b.get_seed_hash() != seed_hash_) throw std::invalid_argument("B seed hash mismatch");
  if (a.get_num_retained() == 0 or b.is_empty()) return compact_theta_sketch_alloc<A>(a, ordered);

  const uint64_t theta = std::min(a.get_theta64(), b.get_theta64());
  uint64_t* keys = nullptr;
  uint32_t keys_size = 0;
  uint32_t count = 0;
  bool is_empty = a.is_empty();

  if (b.get_num_retained() == 0) {
    for (auto key: a) if (key < theta) ++count;
    keys_size = count;
    keys = AllocU64().allocate(keys_size);
    std::copy_if(a.begin(), a.end(), keys, [theta](uint64_t key) { return key < theta; });
    if (ordered and !a.is_ordered()) std::sort(keys, &keys[keys_size]);
    if (count == 0 and theta == theta_sketch_alloc<A>::MAX_THETA) is_empty = true;
    return compact_theta_sketch_alloc<A>(is_empty, theta, keys, count, seed_hash_, a.is_ordered() or ordered);
  }

  keys_size = a.get_num_retained();
  keys = AllocU64().allocate(keys_size);

  if (a.is_ordered() and b.is_ordered()) { // sort-based
    const auto end = std::set_difference(a.begin(), a.end(), b.begin(), b.end(), keys);
    count = end - keys;
  } else { // hash-based
    const uint8_t lg_size = lg_size_from_count(b.get_num_retained(), update_theta_sketch_alloc<A>::REBUILD_THRESHOLD);
    uint64_t* b_hash_table = AllocU64().allocate(1 << lg_size);
    std::fill(b_hash_table, &b_hash_table[1 << lg_size], 0);
    for (auto key: b) {
      if (key < theta) {
        update_theta_sketch_alloc<A>::hash_search_or_insert(key, b_hash_table, lg_size);
      } else if (b.is_ordered()) {
        break; // early stop
      }
    }

    // scan A lookup B
    for (auto key: a) {
      if (key < theta) {
        if (!update_theta_sketch_alloc<A>::hash_search(key, b_hash_table, lg_size)) keys[count++] = key;
      } else if (a.is_ordered()) {
        break; // early stop
      }
    }

    AllocU64().deallocate(b_hash_table, 1 << lg_size);
  }

  if (count == 0) {
    AllocU64().deallocate(keys, keys_size);
    keys = nullptr;
    if (theta == theta_sketch_alloc<A>::MAX_THETA) is_empty = true;
  } else if (count < keys_size) {
    uint64_t* keys_copy = AllocU64().allocate(count);
    std::copy(keys, &keys[count], keys_copy);
    AllocU64().deallocate(keys, keys_size);
    keys = keys_copy;
    if (ordered and !a.is_ordered()) std::sort(keys, &keys[count]);
  }

  return compact_theta_sketch_alloc<A>(is_empty, theta, keys, count, seed_hash_, a.is_ordered() or ordered);
}

} /* namespace datasketches */

# endif
