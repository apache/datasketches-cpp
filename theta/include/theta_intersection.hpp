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

#ifndef THETA_INTERSECTION_HPP_
#define THETA_INTERSECTION_HPP_

#include <memory>
#include <functional>
#include <climits>

#include <theta_sketch.hpp>

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
class theta_intersection_alloc {
public:
  explicit theta_intersection_alloc(uint64_t seed = update_theta_sketch_alloc<A>::builder::DEFAULT_SEED);
  theta_intersection_alloc(const theta_intersection_alloc<A>& other);
  theta_intersection_alloc(theta_intersection_alloc<A>&& other) noexcept;
  ~theta_intersection_alloc();

  theta_intersection_alloc<A>& operator=(theta_intersection_alloc<A> other);
  theta_intersection_alloc<A>& operator=(theta_intersection_alloc<A>&& other);

  void update(const theta_sketch_alloc<A>& sketch);
  compact_theta_sketch_alloc<A> get_result(bool ordered = true) const;
  bool has_result() const;

private:
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
  bool is_valid_;
  bool is_empty_;
  uint64_t theta_;
  uint8_t lg_size_;
  uint64_t* keys_;
  uint32_t num_keys_;
  uint16_t seed_hash_;
};

// alias with default allocator for convenience
typedef theta_intersection_alloc<std::allocator<void>> theta_intersection;

} /* namespace datasketches */

#include "theta_intersection_impl.hpp"

# endif
