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
theta_sketch_experimental<A>::theta_sketch_experimental(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t seed):
table_(lg_cur_size, lg_nom_size, rf, p, seed)
{}

template<typename A>
void theta_sketch_experimental<A>::update(uint64_t key) {
  update(&key, sizeof(key));
}

template<typename A>
void theta_sketch_experimental<A>::update(const void* key, size_t length) {
  const uint64_t hash = compute_hash(key, length, DEFAULT_SEED);
  if (hash >= table_.theta_ || hash == 0) return; // hash == 0 is reserved to mark empty slots in the table
  auto result = table_.find(hash);
  if (!result.second) {
    table_.insert(result.first, hash);
  }
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
auto theta_sketch_experimental<A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  const uint8_t preamble_longs = 3;
  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs + sizeof(uint64_t) * (1 << table_.lg_cur_size_);
  vector_bytes bytes(size);
  uint8_t* ptr = bytes.data() + header_size_bytes;

  const uint8_t preamble_longs_and_rf = preamble_longs | (table_.rf_ << 6);
  ptr += copy_to_mem(&preamble_longs_and_rf, ptr, sizeof(preamble_longs_and_rf));
  const uint8_t serial_version = 0;
  ptr += copy_to_mem(&serial_version, ptr, sizeof(serial_version));
  const uint8_t type = 0;
  ptr += copy_to_mem(&type, ptr, sizeof(type));
  ptr += copy_to_mem(&table_.lg_nom_size_, ptr, sizeof(table_.lg_nom_size_));
  ptr += copy_to_mem(&table_.lg_cur_size_, ptr, sizeof(table_.lg_cur_size_));
  const uint8_t flags_byte(
    (this->is_empty() ? 1 << flags::IS_EMPTY : 0)
  );
  ptr += copy_to_mem(&flags_byte, ptr, sizeof(flags_byte));
  const uint16_t seed_hash = 0;
  ptr += copy_to_mem(&seed_hash, ptr, sizeof(seed_hash));
  ptr += copy_to_mem(&table_.num_entries_, ptr, sizeof(table_.num_entries_));
  const float p = 1;
  ptr += copy_to_mem(&p, ptr, sizeof(p));
  ptr += copy_to_mem(&table_.theta_, ptr, sizeof(table_.theta_));
  ptr += copy_to_mem(table_.entries_, ptr, sizeof(uint64_t) * (1 << table_.lg_cur_size_));

  return bytes;
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
theta_sketch_experimental<A> theta_sketch_experimental<A>::builder::build() const {
  return theta_sketch_experimental(starting_sub_multiple(lg_k_ + 1, MIN_LG_K, static_cast<uint8_t>(rf_)), lg_k_, rf_, p_, seed_);
}

template<typename A>
compact_theta_sketch_experimental<A> theta_sketch_experimental<A>::compact(bool ordered) const {
  return compact_theta_sketch_experimental<A>(*this, ordered);
}

// experimental compact theta sketch

template<typename A>
compact_theta_sketch_experimental<A>::compact_theta_sketch_experimental(const theta_sketch_experimental<A>& other, bool ordered):
is_empty_(other.is_empty()),
is_ordered_(other.is_ordered()),
seed_hash_(other.get_seed_hash()),
theta_(other.get_theta64()),
entries_()
{
  entries_.reserve(other.get_num_retained());
  std::copy(other.begin(), other.end(), std::back_inserter(entries_));
  if (ordered && !other.is_ordered()) std::sort(entries_.begin(), entries_.end());
}

template<typename A>
template<typename InputIt>
compact_theta_sketch_experimental<A>::compact_theta_sketch_experimental(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, InputIt first, InputIt last):
is_empty_(is_empty),
is_ordered_(is_ordered),
seed_hash_(seed_hash),
theta_(theta),
entries_()
{
  std::copy_if(first, last, std::back_inserter(entries_), [theta](uint64_t value) { return value != 0 && value < theta; });
}

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

} /* namespace datasketches */
