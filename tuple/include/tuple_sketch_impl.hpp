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

#include "binomial_bounds.hpp"

namespace datasketches {

template<typename S, typename SD, typename A>
bool tuple_sketch<S, SD, A>::is_estimation_mode() const {
  return get_theta64() < theta_constants::MAX_THETA && !is_empty();
}

template<typename S, typename SD, typename A>
double tuple_sketch<S, SD, A>::get_theta() const {
  return static_cast<double>(get_theta64()) / theta_constants::MAX_THETA;
}

template<typename S, typename SD, typename A>
double tuple_sketch<S, SD, A>::get_estimate() const {
  return get_num_retained() / get_theta();
}

template<typename S, typename SD, typename A>
double tuple_sketch<S, SD, A>::get_lower_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_lower_bound(get_num_retained(), get_theta(), num_std_devs);
}

template<typename S, typename SD, typename A>
double tuple_sketch<S, SD, A>::get_upper_bound(uint8_t num_std_devs) const {
  if (!is_estimation_mode()) return get_num_retained();
  return binomial_bounds::get_upper_bound(get_num_retained(), get_theta(), num_std_devs);
}

// update sketch

template<typename S, typename U, typename P, typename SD, typename A>
update_tuple_sketch<S, U, P, SD, A>::update_tuple_sketch(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t seed, const P& policy):
policy_(policy),
map_(lg_cur_size, lg_nom_size, rf, p, seed)
{}

template<typename S, typename U, typename P, typename SD, typename A>
bool update_tuple_sketch<S, U, P, SD, A>::is_empty() const {
  return map_.is_empty_;
}

template<typename S, typename U, typename P, typename SD, typename A>
bool update_tuple_sketch<S, U, P, SD, A>::is_ordered() const {
  return false;
}

template<typename S, typename U, typename P, typename SD, typename A>
uint64_t update_tuple_sketch<S, U, P, SD, A>::get_theta64() const {
  return map_.theta_;
}

template<typename S, typename U, typename P, typename SD, typename A>
uint32_t update_tuple_sketch<S, U, P, SD, A>::get_num_retained() const {
  return map_.num_entries_;
}

template<typename S, typename U, typename P, typename SD, typename A>
uint16_t update_tuple_sketch<S, U, P, SD, A>::get_seed_hash() const {
  return compute_seed_hash(map_.seed_);
}

template<typename S, typename U, typename P, typename SD, typename A>
uint8_t update_tuple_sketch<S, U, P, SD, A>::get_lg_k() const {
  return map_.lg_nom_size_;
}

template<typename S, typename U, typename P, typename SD, typename A>
auto update_tuple_sketch<S, U, P, SD, A>::get_rf() const -> resize_factor {
  return map_.rf_;
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(const std::string& key, UU&& value) {
  if (key.empty()) return;
  update(key.c_str(), key.length(), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(uint64_t key, UU&& value) {
  update(&key, sizeof(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(int64_t key, UU&& value) {
  update(&key, sizeof(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(uint32_t key, UU&& value) {
  update(static_cast<int32_t>(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(int32_t key, UU&& value) {
  update(static_cast<int64_t>(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(uint16_t key, UU&& value) {
  update(static_cast<int16_t>(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(int16_t key, UU&& value) {
  update(static_cast<int64_t>(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(uint8_t key, UU&& value) {
  update(static_cast<int8_t>(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(double key, UU&& value) {
  update(canonical_double(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(float key, UU&& value) {
  update(static_cast<double>(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(int8_t key, UU&& value) {
  update(static_cast<int64_t>(key), std::forward<UU>(value));
}

template<typename S, typename U, typename P, typename SD, typename A>
template<typename UU>
void update_tuple_sketch<S, U, P, SD, A>::update(const void* key, size_t length, UU&& value) {
  const uint64_t hash = map_.hash_and_screen(key, length);
  if (hash == 0) return;
  auto result = map_.find(hash);
  if (!result.second) {
    S summary = policy_.create();
    policy_.update(summary, std::forward<UU>(value));
    map_.insert(result.first, Entry(hash, std::move(summary)));
  } else {
    policy_.update((*result.first).second, std::forward<UU>(value));
  }
}

template<typename S, typename U, typename P, typename SD, typename A>
void update_tuple_sketch<S, U, P, SD, A>::trim() {
  map_.trim();
}

template<typename S, typename U, typename P, typename SD, typename A>
string<A> update_tuple_sketch<S, U, P, SD, A>::to_string(bool detail) const {
  std::basic_ostringstream<char, std::char_traits<char>, AllocChar<A>> os;
  auto type = typeid(*this).name();
  os << "sizeof(" << type << ")=" << sizeof(*this) << std::endl;
  os << "sizeof(entry)=" << sizeof(Entry) << std::endl;
  os << map_.to_string();
  if (detail) {
    for (const auto& it: map_) {
      if (it.first != 0) {
        os << it.first << ": " << it.second << std::endl;
      }
    }
  }
  return os.str();
}

template<typename S, typename U, typename P, typename SD, typename A>
auto update_tuple_sketch<S, U, P, SD, A>::begin() const -> const_iterator {
  return const_iterator(map_.entries_, 1 << map_.lg_cur_size_, 0);
}

template<typename S, typename U, typename P, typename SD, typename A>
auto update_tuple_sketch<S, U, P, SD, A>::end() const -> const_iterator {
  return const_iterator(nullptr, 0, 1 << map_.lg_cur_size_);
}

template<typename S, typename U, typename P, typename SD, typename A>
compact_tuple_sketch<S, SD, A> update_tuple_sketch<S, U, P, SD, A>::compact(bool ordered) const {
  return compact_tuple_sketch<S, SD, A>(*this, ordered);
}

// compact sketch

template<typename S, typename SD, typename A>
compact_tuple_sketch<S, SD, A>::compact_tuple_sketch(bool is_empty, bool is_ordered, uint16_t seed_hash, uint64_t theta, std::vector<Entry, AllocEntry>&& entries):
is_empty_(is_empty),
is_ordered_(is_ordered),
seed_hash_(seed_hash),
theta_(theta),
entries_(std::move(entries))
{}

template<typename S, typename SD, typename A>
compact_tuple_sketch<S, SD, A>::compact_tuple_sketch(const Base& other, bool ordered):
is_empty_(other.is_empty()),
is_ordered_(other.is_ordered() || ordered),
seed_hash_(other.get_seed_hash()),
theta_(other.get_theta64()),
entries_()
{
  entries_.reserve(other.get_num_retained());
  std::copy(other.begin(), other.end(), std::back_inserter(entries_));
  if (ordered && !other.is_ordered()) std::sort(entries_.begin(), entries_.end(), comparator());
}

template<typename S, typename SD, typename A>
bool compact_tuple_sketch<S, SD, A>::is_empty() const {
  return is_empty_;
}

template<typename S, typename SD, typename A>
bool compact_tuple_sketch<S, SD, A>::is_ordered() const {
  return is_ordered_;
}

template<typename S, typename SD, typename A>
uint64_t compact_tuple_sketch<S, SD, A>::get_theta64() const {
  return theta_;
}

template<typename S, typename SD, typename A>
uint32_t compact_tuple_sketch<S, SD, A>::get_num_retained() const {
  return entries_.size();
}

template<typename S, typename SD, typename A>
uint16_t compact_tuple_sketch<S, SD, A>::get_seed_hash() const {
  return seed_hash_;
}

template<typename S, typename SD, typename A>
string<A> compact_tuple_sketch<S, SD, A>::to_string(bool detail) const {
  std::basic_ostringstream<char, std::char_traits<char>, AllocChar<A>> os;
  os << "### Compact Tuple sketch summary:" << std::endl;
  auto type = typeid(*this).name();
  os << "   type                 : " << type << std::endl;
  os << "   sizeof(type)         : " << sizeof(*this) << std::endl;
  os << "   sizeof(entry)        : " << sizeof(Entry) << std::endl;
  os << "   num retained entries : " << entries_.size() << std::endl;
  os << "   seed hash            : " << this->get_seed_hash() << std::endl;
  os << "   empty?               : " << (this->is_empty() ? "true" : "false") << std::endl;
  os << "   ordered?             : " << (this->is_ordered() ? "true" : "false") << std::endl;
  os << "   estimation mode?     : " << (this->is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   theta (fraction)     : " << this->get_theta() << std::endl;
  os << "   theta (raw 64-bit)   : " << this->theta_ << std::endl;
  os << "   estimate             : " << this->get_estimate() << std::endl;
  os << "   lower bound 95% conf : " << this->get_lower_bound(2) << std::endl;
  os << "   upper bound 95% conf : " << this->get_upper_bound(2) << std::endl;
  os << "### End sketch summary" << std::endl;
  if (detail) {
    os << "### Retained entries" << std::endl;
    for (const auto& it: entries_) {
      if (it.first != 0) {
        os << it.first << ": " << it.second << std::endl;
      }
    }
    os << "### End retained entries" << std::endl;
  }
  return os.str();
}

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename S, typename SD, typename A>
template<typename SS, typename std::enable_if<std::is_arithmetic<SS>::value, int>::type>
size_t compact_tuple_sketch<S, SD, A>::get_serialized_size_summaries_bytes() const {
  return entries_.size() * sizeof(SS);
}

// implementation for all other types (non-arithmetic)
template<typename S, typename SD, typename A>
template<typename SS, typename std::enable_if<!std::is_arithmetic<SS>::value, int>::type>
size_t compact_tuple_sketch<S, SD, A>::get_serialized_size_summaries_bytes() const {
  size_t size = 0;
  for (const auto& it: entries_) {
    size += SD().size_of_item(it.second);
  }
  return size;
}

template<typename S, typename SD, typename A>
void compact_tuple_sketch<S, SD, A>::serialize(std::ostream& os) const {
}

template<typename S, typename SD, typename A>
auto compact_tuple_sketch<S, SD, A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  const bool is_single_item = entries_.size() == 1 && !this->is_estimation_mode();
  const uint8_t preamble_longs = this->is_empty() || is_single_item ? 1 : this->is_estimation_mode() ? 3 : 2;
  const size_t size = header_size_bytes + sizeof(uint64_t) * preamble_longs
      + sizeof(uint64_t) * entries_.size() + get_serialized_size_summaries_bytes();
  vector_bytes bytes(size);
  uint8_t* ptr = bytes.data() + header_size_bytes;
  const uint8_t* end_ptr = ptr + size;

  ptr += copy_to_mem(&preamble_longs, ptr, sizeof(preamble_longs));
  const uint8_t serial_version = Base::SERIAL_VERSION;
  ptr += copy_to_mem(&serial_version, ptr, sizeof(serial_version));
  const uint8_t type = SKETCH_TYPE;
  ptr += copy_to_mem(&type, ptr, sizeof(type));
  const uint16_t unused16 = 0;
  ptr += copy_to_mem(&unused16, ptr, sizeof(unused16));
  const uint8_t flags_byte(
    (1 << Base::flags::IS_COMPACT) |
    (1 << Base::flags::IS_READ_ONLY) |
    (this->is_empty() ? 1 << Base::flags::IS_EMPTY : 0) |
    (this->is_ordered() ? 1 << Base::flags::IS_ORDERED : 0)
  );
  ptr += copy_to_mem(&flags_byte, ptr, sizeof(flags_byte));
  const uint16_t seed_hash = get_seed_hash();
  ptr += copy_to_mem(&seed_hash, ptr, sizeof(seed_hash));
  if (!this->is_empty()) {
    if (!is_single_item) {
      const uint32_t num_entries = entries_.size();
      ptr += copy_to_mem(&num_entries, ptr, sizeof(num_entries));
      const uint32_t unused32 = 0;
      ptr += copy_to_mem(&unused32, ptr, sizeof(unused32));
      if (this->is_estimation_mode()) {
        ptr += copy_to_mem(&theta_, ptr, sizeof(uint64_t));
      }
    }
    for (const auto& it: entries_) {
      ptr += copy_to_mem(&it.first, ptr, sizeof(uint64_t));
    }
    for (const auto& it: entries_) {
      ptr += SD().serialize(ptr, end_ptr - ptr, &it.second, 1);
    }
  }
  return bytes;
}

template<typename S, typename SD, typename A>
auto compact_tuple_sketch<S, SD, A>::begin() const -> const_iterator {
  return const_iterator(entries_.data(), entries_.size(), 0);
}

template<typename S, typename SD, typename A>
auto compact_tuple_sketch<S, SD, A>::end() const -> const_iterator {
  return const_iterator(nullptr, 0, entries_.size());
}

// builder

template<typename S, typename U, typename P, typename SD, typename A>
update_tuple_sketch<S, U, P, SD, A>::builder::builder(const P& policy):
policy_(policy) {}

template<typename S, typename U, typename P, typename SD, typename A>
update_tuple_sketch<S, U, P, SD, A> update_tuple_sketch<S, U, P, SD, A>::builder::build() const {
  return update_tuple_sketch(this->starting_lg_size(), this->lg_k_, this->rf_, this->p_, this->seed_, policy_);
}

} /* namespace datasketches */
