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

template<typename Summary, typename Allocator>
theta_to_tuple_sketch_adapter<Summary, Allocator>::theta_to_tuple_sketch_adapter(const update_theta_sketch_experimental<Allocator>& sketch, const Summary& summary):
sketch_ptr(&sketch), summary(summary) {}

template<typename Summary, typename Allocator>
theta_to_tuple_sketch_adapter<Summary, Allocator>::theta_to_tuple_sketch_adapter(const compact_theta_sketch_experimental<Allocator>& sketch, const Summary& summary):
sketch_ptr(&sketch), summary(summary) {}

template<typename Summary, typename Allocator>
bool theta_to_tuple_sketch_adapter<Summary, Allocator>::is_empty() const {
  return sketch_ptr->is_empty();
}

template<typename Summary, typename Allocator>
bool theta_to_tuple_sketch_adapter<Summary, Allocator>::is_ordered() const {
  return sketch_ptr->is_ordered();
}

template<typename Summary, typename Allocator>
uint16_t theta_to_tuple_sketch_adapter<Summary, Allocator>::get_seed_hash() const {
  return sketch_ptr->get_seed_hash();
}

template<typename Summary, typename Allocator>
uint64_t theta_to_tuple_sketch_adapter<Summary, Allocator>::get_theta64() const {
  return sketch_ptr->get_theta64();
}

template<typename Summary, typename Allocator>
auto theta_to_tuple_sketch_adapter<Summary, Allocator>::begin() -> const_iterator {
  return const_iterator(sketch_ptr->begin(), summary);
}

template<typename Summary, typename Allocator>
auto theta_to_tuple_sketch_adapter<Summary, Allocator>::end() -> const_iterator {
  return const_iterator(sketch_ptr->end(), summary);
}

template<typename Summary, typename Allocator>
theta_to_tuple_sketch_adapter<Summary, Allocator>::const_iterator::const_iterator(const theta_const_iterator& it, const Summary& summary):
it(it), summary(summary), entry(0, summary) {}

template<typename Summary, typename Allocator>
auto theta_to_tuple_sketch_adapter<Summary, Allocator>::const_iterator::operator++() -> const_iterator& {
  ++it;
  return *this;
}

template<typename Summary, typename Allocator>
auto theta_to_tuple_sketch_adapter<Summary, Allocator>::const_iterator::operator++(int) -> const_iterator {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename Summary, typename Allocator>
bool theta_to_tuple_sketch_adapter<Summary, Allocator>::const_iterator::operator==(const const_iterator& other) const {
  return this->it == other.it;
}

template<typename Summary, typename Allocator>
bool theta_to_tuple_sketch_adapter<Summary, Allocator>::const_iterator::operator!=(const const_iterator& other) const {
  return this->it != other.it;
}

template<typename Summary, typename Allocator>
auto theta_to_tuple_sketch_adapter<Summary, Allocator>::const_iterator::operator*() const -> Entry& {
  entry = Entry(*it, summary); // fresh entry every time
  return entry;
}

} /* namespace datasketches */
