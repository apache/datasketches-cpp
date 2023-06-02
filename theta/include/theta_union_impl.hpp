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

#ifndef THETA_UNION_IMPL_HPP_
#define THETA_UNION_IMPL_HPP_

#include <utility>

namespace datasketches {

template<typename A, template<typename, typename, typename> class T>
theta_union_alloc<A, T>::theta_union_alloc(theta_table&& table):
state_{nop_policy(), std::forward<T<Entry,ExtractKey,A>>(table)}
{}

template<typename A, template<typename, typename, typename> class T>
template<typename SS>
void theta_union_alloc<A, T>::update(SS&& sketch) {
  state_.update(std::forward<SS>(sketch));
}

template<typename A, template<typename, typename, typename> class T>
auto theta_union_alloc<A, T>::get_result(bool ordered) const -> CompactSketch {
  return state_.get_result(ordered);
}

template<typename A, template<typename, typename, typename> class T>
void theta_union_alloc<A, T>::reset() {
  state_.reset();
}

template<typename A, template<typename, typename, typename> class T>
theta_union_alloc<A, T>::builder::builder(const A& allocator): theta_base_builder<builder, A>(allocator) {}

template<typename A, template<typename, typename, typename> class T>
auto theta_union_alloc<A, T>::builder::build() const -> theta_union_alloc {
  return theta_union_alloc(Table(this->starting_lg_size(), this->lg_k_, this->rf_, this->p_, this->starting_theta(), this->seed_, this->allocator_));
}

} /* namespace datasketches */

# endif
