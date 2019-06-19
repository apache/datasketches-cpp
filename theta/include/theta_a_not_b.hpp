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

#ifndef THETA_A_NOT_B_HPP_
#define THETA_A_NOT_B_HPP_

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
class theta_a_not_b_alloc {
public:
  explicit theta_a_not_b_alloc(uint64_t seed = update_theta_sketch_alloc<A>::builder::DEFAULT_SEED);

  compact_theta_sketch_alloc<A> compute(const theta_sketch_alloc<A>& a, const theta_sketch_alloc<A>& b, bool ordered = true) const;

private:
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
  uint16_t seed_hash_;

};

// alias with default allocator for convenience
typedef theta_a_not_b_alloc<std::allocator<void>> theta_a_not_b;

} /* namespace datasketches */

#include "theta_a_not_b_impl.hpp"

# endif
