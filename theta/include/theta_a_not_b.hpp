/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
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
