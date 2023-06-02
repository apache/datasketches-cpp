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

template<typename S, typename P, typename A, template<typename, typename, typename> class T>
tuple_union<S, P, A, T>::tuple_union(const P& policy, theta_table&& table):
state_{internal_policy(policy), std::forward<T<Entry,ExtractKey,AllocEntry>>(table)}
{}

template<typename S, typename P, typename A, template<typename, typename, typename> class T>
template<typename SS>
void tuple_union<S, P, A, T>::update(SS&& sketch) {
  state_.update(std::forward<SS>(sketch));
}

template<typename S, typename P, typename A, template<typename, typename, typename> class T>
auto tuple_union<S, P, A, T>::get_result(bool ordered) const -> CompactSketch {
  return state_.get_result(ordered);
}

template<typename S, typename P, typename A, template<typename, typename, typename> class T>
void tuple_union<S, P, A, T>::reset() {
  return state_.reset();
}

template<typename S, typename P, typename A, template<typename, typename, typename> class T>
tuple_union<S, P, A, T>::builder::builder(const P& policy, const A& allocator):
tuple_base_builder<builder, P, A>(policy, allocator) {}

template<typename S, typename P, typename A, template<typename, typename, typename> class T>
auto tuple_union<S, P, A, T>::builder::build() const -> tuple_union {
  return tuple_union(this->policy_, Table(this->starting_lg_size(), this->lg_k_, this->rf_, this->p_, this->starting_theta(), this->seed_, this->allocator_));
}

} /* namespace datasketches */
