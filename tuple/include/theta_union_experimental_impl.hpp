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

namespace datasketches {

template<typename A>
theta_union_experimental<A>::theta_union_experimental(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t seed):
state_(lg_cur_size, lg_nom_size, rf, p, seed, pass_through_policy())
{}

template<typename A>
void theta_union_experimental<A>::update(const Sketch& sketch) {
  state_.update(sketch);
}

template<typename A>
auto theta_union_experimental<A>::get_result(bool ordered) const -> CompactSketch {
  return state_.get_result(ordered);
}

template<typename A>
auto theta_union_experimental<A>::builder::build() const -> theta_union_experimental {
  return theta_union_experimental(starting_sub_multiple(lg_k_ + 1, MIN_LG_K, static_cast<uint8_t>(rf_)), lg_k_, rf_, p_, seed_);
}

} /* namespace datasketches */
