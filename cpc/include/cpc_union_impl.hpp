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

#ifndef CPC_UNION_IMPL_HPP_
#define CPC_UNION_IMPL_HPP_

namespace datasketches {

template<typename A>
cpc_union_alloc<A>::cpc_union_alloc(uint8_t lg_k, uint64_t seed) : seed(seed) {
  fm85Init();
  if (lg_k < CPC_MIN_LG_K or lg_k > CPC_MAX_LG_K) {
    throw std::invalid_argument("lg_k must be >= " + std::to_string(CPC_MIN_LG_K) + " and <= " + std::to_string(CPC_MAX_LG_K) + ": " + std::to_string(lg_k));
  }
  state = ug85Make(lg_k);
}

template<typename A>
cpc_union_alloc<A>::cpc_union_alloc(const cpc_union_alloc<A>& other) {
  seed = other.seed;
  state = ug85Copy(other.state);
}

template<typename A>
cpc_union_alloc<A>& cpc_union_alloc<A>::operator=(cpc_union_alloc<A> other) {
  seed = other.seed;
  std::swap(state, other.state); // @suppress("Invalid arguments")
  return *this;
}

template<typename A>
cpc_union_alloc<A>::~cpc_union_alloc() {
  ug85Free(state);
}

template<typename A>
void cpc_union_alloc<A>::update(const cpc_sketch_alloc<A>& sketch) {
  const uint16_t seed_hash_union = compute_seed_hash(seed);
  const uint16_t seed_hash_sketch = compute_seed_hash(sketch.seed);
  if (seed_hash_union != seed_hash_sketch) {
    throw std::invalid_argument("Incompatible seed hashes: " + std::to_string(seed_hash_union) + ", "
        + std::to_string(seed_hash_sketch));
  }
  ug85MergeInto(state, sketch.state);
}

template<typename A>
cpc_sketch_alloc<A> cpc_union_alloc<A>::get_result() const {
  return cpc_sketch_alloc<A>(ug85GetResult(state), seed);
}

} /* namespace datasketches */

#endif
