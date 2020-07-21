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

#include <sstream>

namespace datasketches {

// experimental update theta sketch derived from the same base as tuple sketch

template<typename A>
theta_sketch_experimental<A>::theta_sketch_experimental(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf,
    uint64_t theta, uint64_t seed, const A& allocator):
table_(lg_cur_size, lg_nom_size, rf, theta, seed, allocator)
{}

template<typename A>
void theta_sketch_experimental<A>::update(uint64_t key) {
  update(&key, sizeof(key));
}

template<typename A>
void theta_sketch_experimental<A>::update(const void* key, size_t length) {
  const uint64_t hash = table_.hash_and_screen(key, length);
  if (hash == 0) return;
  auto result = table_.find(hash);
  if (!result.second) {
    table_.insert(result.first, hash);
  }
}

template<typename A>
void theta_sketch_experimental<A>::trim() {
  table_.trim();
}

template<typename A>
string<A> theta_sketch_experimental<A>::to_string(bool detail) const {
  std::basic_ostringstream<char, std::char_traits<char>, AllocChar<A>> os;
  auto type = typeid(*this).name();
  os << "sizeof(" << type << ")=" << sizeof(*this) << std::endl;
  os << table_.to_string();
  if (detail) {
    for (const auto& it: table_) {
      if (it != 0) {
        os << it << std::endl;
      }
    }
  }
  return os.str();
}

template<typename A>
auto theta_sketch_experimental<A>::begin() const -> const_iterator {
  return const_iterator(table_.entries_, 1 << table_.lg_cur_size_, 0);
}

template<typename A>
auto theta_sketch_experimental<A>::end() const -> const_iterator {
  return const_iterator(nullptr, 0, 1 << table_.lg_cur_size_);
}

template<typename A>
compact_theta_sketch_experimental<A> theta_sketch_experimental<A>::compact(bool ordered) const {
  return compact_theta_sketch_experimental<A>(*this, ordered);
}

// builder

template<typename A>
theta_sketch_experimental<A>::builder::builder(const A& allocator): allocator_(allocator) {}

template<typename A>
theta_sketch_experimental<A> theta_sketch_experimental<A>::builder::build() const {
  return theta_sketch_experimental(this->starting_lg_size(), this->lg_k_, this->rf_, this->starting_theta(), this->seed_, allocator_);
}

// experimental compact theta sketch

template<typename A>
compact_theta_sketch_experimental<A>::compact_theta_sketch_experimental(const theta_sketch_experimental<A>& other, bool ordered):
is_empty_(other.is_empty()),
is_ordered_(other.is_ordered()),
seed_hash_(other.get_seed_hash()),
theta_(other.get_theta64()),
entries_(other.get_allocator())
{
  entries_.reserve(other.get_num_retained());
  std::copy(other.begin(), other.end(), std::back_inserter(entries_));
  if (ordered && !other.is_ordered()) std::sort(entries_.begin(), entries_.end());
}

template<typename A>
compact_theta_sketch_experimental<A>::compact_theta_sketch_experimental(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta,
    std::vector<uint64_t, A>&& entries):
is_empty_(is_empty),
is_ordered_(is_ordered),
seed_hash_(seed_hash),
theta_(theta),
entries_(std::move(entries))
{}

template<typename A>
string<A> compact_theta_sketch_experimental<A>::to_string(bool detail) const {
  std::basic_ostringstream<char, std::char_traits<char>, AllocChar<A>> os;
  auto type = typeid(*this).name();
  os << "sizeof(" << type << ")=" << sizeof(*this) << std::endl;
  if (detail) {
    for (const auto& hash: entries_) {
      os << hash << std::endl;
    }
  }
  return os.str();
}

template<typename A>
A compact_theta_sketch_experimental<A>::get_allocator() const {
  return entries_.get_allocator();
}

} /* namespace datasketches */
