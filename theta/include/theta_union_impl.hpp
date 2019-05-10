/*
 * Copyright 2019, Verizon Media.
 * Licensed under the terms of the Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef THETA_UNION_IMPL_HPP_
#define THETA_UNION_IMPL_HPP_

namespace datasketches {

/*
 * author Alexander Saydakov
 * author Lee Rhodes
 * author Kevin Lang
 */

template<typename A>
theta_union_alloc<A>::theta_union_alloc(uint64_t theta, update_theta_sketch_alloc<A>&& state):
theta_(theta), state_(std::move(state)) {}

template<typename A>
void theta_union_alloc<A>::update(const theta_sketch_alloc<A>& sketch) {
  if (sketch.is_empty()) return;
  // check seed hash
  if (sketch.get_theta64() < theta_) theta_ = sketch.get_theta64();
  if (sketch.is_ordered()) {
    for (auto hash: sketch) {
      if (hash >= theta_) break; // early stop
      state_.internal_update(hash);
    }
  } else {
    for (auto hash: sketch) if (hash < theta_) state_.internal_update(hash);
  }
}

template<typename A>
compact_theta_sketch_alloc<A> theta_union_alloc<A>::get_result(bool ordered) const {
  if (state_.is_empty()) return state_.compact(ordered);
  const uint32_t nom_num_keys = 1 << state_.lg_nom_size_;
  if (theta_ >= state_.theta_ and state_.get_num_retained() <= nom_num_keys) return state_.compact(ordered);
  uint64_t theta = std::min(theta_, state_.get_theta64());
  typedef typename std::allocator_traits<A>::template rebind_alloc<uint64_t> AllocU64;
  uint64_t* keys = AllocU64().allocate(state_.get_num_retained());
  uint32_t num_keys = 0;
  for (auto key: state_) {
    if (key < theta) keys[num_keys++] = key;
  }
  if (num_keys == 0) return compact_theta_sketch_alloc<A>(true, theta_sketch_alloc<A>::MAX_THETA, nullptr, 0, state_.get_seed_hash(), true);
  if (num_keys > nom_num_keys) {
    std::nth_element(keys, &keys[nom_num_keys], &keys[num_keys]);
    theta = keys[nom_num_keys];
    num_keys = nom_num_keys;
  }
  if (num_keys != state_.get_num_retained()) {
    uint64_t* new_keys = AllocU64().allocate(num_keys);
    std::copy(keys, &keys[num_keys], new_keys);
    AllocU64().deallocate(keys, state_.get_num_retained());
    keys = new_keys;
  }
  if (ordered) std::sort(keys, &keys[num_keys]);
  return compact_theta_sketch_alloc<A>(false, theta, keys, num_keys, state_.get_seed_hash(), ordered);
}

// builder

template<typename A>
typename theta_union_alloc<A>::builder& theta_union_alloc<A>::builder::set_lg_k(uint8_t lg_k) {
  sketch_builder.set_lg_k(lg_k);
  return *this;
}

template<typename A>
typename theta_union_alloc<A>::builder& theta_union_alloc<A>::builder::set_resize_factor(resize_factor rf) {
  sketch_builder.set_resize_factor(rf);
  return *this;
}

template<typename A>
typename theta_union_alloc<A>::builder& theta_union_alloc<A>::builder::set_p(float p) {
  sketch_builder.set_p(p);
  return *this;
}

template<typename A>
typename theta_union_alloc<A>::builder& theta_union_alloc<A>::builder::set_seed(uint64_t seed) {
  sketch_builder.set_seed(seed);
  return *this;
}

template<typename A>
theta_union_alloc<A> theta_union_alloc<A>::builder::build() const {
  update_theta_sketch_alloc<A> sketch = sketch_builder.build();
  return theta_union_alloc(sketch.get_theta64(), std::move(sketch));
}

} /* namespace datasketches */

# endif
