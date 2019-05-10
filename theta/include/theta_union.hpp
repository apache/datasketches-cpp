/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
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
