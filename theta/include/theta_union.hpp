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

#ifndef THETA_UNION_HPP_
#define THETA_UNION_HPP_

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
class theta_union_alloc {
public:
  class builder;
  void update(const theta_sketch_alloc<A>& sketch);
  compact_theta_sketch_alloc<A> get_result(bool ordered = true) const;

private:
  bool is_empty_;
  uint64_t theta_;
  update_theta_sketch_alloc<A> state_;

  // for builder
  theta_union_alloc(uint64_t theta, update_theta_sketch_alloc<A>&& state);
};

// builder

template<typename A>
class theta_union_alloc<A>::builder {
public:
  typedef typename update_theta_sketch_alloc<A>::resize_factor resize_factor;
  builder& set_lg_k(uint8_t lg_k);
  builder& set_resize_factor(resize_factor rf);
  builder& set_p(float p);
  builder& set_seed(uint64_t seed);
  theta_union_alloc<A> build() const;
private:
  typename update_theta_sketch_alloc<A>::builder sketch_builder;
};

// alias with default allocator for convenience
typedef theta_union_alloc<std::allocator<void>> theta_union;

} /* namespace datasketches */

#include "theta_union_impl.hpp"

# endif
