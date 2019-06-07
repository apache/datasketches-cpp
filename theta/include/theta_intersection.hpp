/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
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
