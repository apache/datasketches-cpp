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
  static const uint64_t MAX_THETA = LLONG_MAX; // signed max for compatibility with Java

  theta_union_alloc(uint8_t lg_k);

  bool is_empty() const;
  void update(const theta_sketch_alloc<A>& sketch);
  compact_theta_sketch_alloc<A> get_result(bool ordered = true) const;
  //void to_stream(std::ostream& os, bool print_items = false) const;

private:

  uint64_t theta_;
  update_theta_sketch_alloc<A> state_;
};

// alias with default allocator for convenience
typedef theta_union_alloc<std::allocator<void>> theta_union;

} /* namespace datasketches */

#include "theta_union_impl.hpp"

# endif
