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

#ifndef REQ_COMPACTOR_IMPL_HPP_
#define REQ_COMPACTOR_IMPL_HPP_

#include <stdexcept>
#include <cmath>
#include <algorithm>

#include "count_zeros.hpp"
#include "conditional_forward.hpp"

namespace datasketches {

template<typename T, bool H, typename C, typename A>
req_compactor<T, H, C, A>::req_compactor(uint8_t lg_weight, uint32_t section_size, const A& allocator, bool sorted):
lg_weight_(lg_weight),
coin_(false),
sorted_(sorted),
section_size_raw_(section_size),
section_size_(section_size),
num_sections_(req_constants::INIT_NUM_SECTIONS),
state_(0),
items_(allocator)
{
  items_.reserve(2 * get_nom_capacity());
}

template<typename T, bool H, typename C, typename A>
bool req_compactor<T, H, C, A>::is_sorted() const {
  return sorted_;
}

template<typename T, bool H, typename C, typename A>
uint32_t req_compactor<T, H, C, A>::get_num_items() const {
  return items_.size();
}

template<typename T, bool H, typename C, typename A>
uint32_t req_compactor<T, H, C, A>::get_nom_capacity() const {
  return 2 * num_sections_ * section_size_;
}

template<typename T, bool H, typename C, typename A>
uint8_t req_compactor<T, H, C, A>::get_lg_weight() const {
  return lg_weight_;
}

template<typename T, bool H, typename C, typename A>
std::vector<T, A>& req_compactor<T, H, C, A>::get_items() {
  return items_;
}

template<typename T, bool H, typename C, typename A>
const std::vector<T, A>& req_compactor<T, H, C, A>::get_items() const {
  return items_;
}

template<typename T, bool H, typename C, typename A>
template<bool inclusive>
uint64_t req_compactor<T, H, C, A>::compute_weight(const T& item) const {
  if (!sorted_) const_cast<req_compactor*>(this)->sort(); // allow sorting as a side effect
  auto it = inclusive ?
      std::upper_bound(items_.begin(), items_.end(), item, C()) :
      std::lower_bound(items_.begin(), items_.end(), item, C());
  return std::distance(items_.begin(), it) << lg_weight_;
}

template<typename T, bool H, typename C, typename A>
template<typename FwdT>
void req_compactor<T, H, C, A>::append(FwdT&& item) {
  items_.push_back(std::forward<FwdT>(item));
  sorted_ = false;
}

template<typename T, bool H, typename C, typename A>
template<typename FwdC>
void req_compactor<T, H, C, A>::merge(FwdC&& other) {
  if (lg_weight_ != other.lg_weight_) throw std::logic_error("weight mismatch");
  state_ |= other.state_;
  while (ensure_enough_sections()) {}
  sort();
  std::vector<T, A> other_items(conditional_forward<FwdC>(other.items_));
  if (!other.sorted_) std::sort(other_items.begin(), other_items.end(), C());
  if (other_items.size() > items_.size()) std::swap(items_, other_items);
  merge_sort_in(std::move(other_items));
}

template<typename T, bool H, typename C, typename A>
void req_compactor<T, H, C, A>::sort() {
  if (!sorted_) {
    std::sort(items_.begin(), items_.end(), C());
    sorted_ = true;
  }
}

template<typename T, bool H, typename C, typename A>
void req_compactor<T, H, C, A>::merge_sort_in(std::vector<T, A>&& items) {
  if (!sorted_) throw std::logic_error("compactor must be sorted at this point");
  if (items_.capacity() < items_.size() + items.size()) items_.reserve(items_.size() + items.size());
  auto middle = items_.end();
  std::move(items.begin(), items.end(), std::back_inserter(items_));
  std::inplace_merge(items_.begin(), middle, items_.end(), C());

  // alternative implementation
  //  std::vector<T, A> merged(items_.get_allocator());
  //  merged.reserve(items_.size() + items.size());
  //  std::merge(
  //      std::make_move_iterator(items_.begin()), std::make_move_iterator(items_.end()),
  //      std::make_move_iterator(items.begin()), std::make_move_iterator(items.end()),
  //      std::back_inserter(merged), C()
  //  );
  //  std::swap(items_, merged);
}

template<typename T, bool H, typename C, typename A>
void req_compactor<T, H, C, A>::compact(req_compactor& next) {
  // choose a part of the buffer to compact
  const uint32_t secs_to_compact = std::min(static_cast<uint32_t>(count_trailing_zeros_in_u32(~state_) + 1), static_cast<uint32_t>(num_sections_));
  const size_t compaction_range = compute_compaction_range(secs_to_compact);
  const uint32_t compact_from = compaction_range & 0xFFFFFFFFLL; // low 32
  const uint32_t compact_to = compaction_range >> 32; // high 32
  if (compact_to - compact_from < 2) throw std::logic_error("compaction range error");

  if ((state_ & 1) == 1) { coin_ = !coin_; } // for odd flip coin;
  else { coin_ = req_random_bit(); } // random coin flip

  auto& dst = next.get_items();
  const auto num = (compact_to - compact_from) / 2;
  if (dst.size() + num > dst.capacity()) dst.reserve(dst.size() + num);
  auto middle = dst.end();
  promote_evens_or_odds(items_.begin() + compact_from, items_.begin() + compact_to, coin_, std::back_inserter(dst));
  std::inplace_merge(dst.begin(), middle, dst.end(), C());
  items_.erase(items_.begin() + compact_from, items_.begin() + compact_to);

  ++state_;
  ensure_enough_sections();
}

template<typename T, bool H, typename C, typename A>
bool req_compactor<T, H, C, A>::ensure_enough_sections() {
  const float ssr = section_size_raw_ / sqrt(2);
  const uint32_t ne = nearest_even(ssr);
  if (state_ >= 1 << (num_sections_ - 1) && ne >= req_constants::MIN_K) {
    section_size_raw_ = ssr;
    section_size_ = ne;
    num_sections_ <<= 1;
    items_.reserve(2 * get_nom_capacity());
    return true;
  }
  return false;
}

template<typename T, bool H, typename C, typename A>
size_t req_compactor<T, H, C, A>::compute_compaction_range(uint32_t secs_to_compact) const {
  const uint32_t num_items = items_.size();
  uint32_t non_compact = get_nom_capacity() / 2 + (num_sections_ - secs_to_compact) * section_size_;
  // make compacted region even
  if ((num_items - non_compact & 1) == 1) ++non_compact;
  const size_t low = H ? 0 : non_compact;
  const size_t high = H ? num_items - non_compact : num_items;
  return (high << 32) + low;
}

template<typename T, bool H, typename C, typename A>
uint32_t req_compactor<T, H, C, A>::nearest_even(float value) {
  return static_cast<uint32_t>(round(value / 2)) << 1;
}

template<typename T, bool H, typename C, typename A>
template<typename InIter, typename OutIter>
void req_compactor<T, H, C, A>::promote_evens_or_odds(InIter from, InIter to, bool odds, OutIter dst) {
  if (from == to) return;
  InIter i = from;
  if (odds) ++i;
  while (i != to) {
    dst = std::move(*i);
    ++dst;
    ++i;
    if (i == to) break;
    ++i;
  }
}

// helpers for integral types
template<typename T>
static inline T read(std::istream& is) {
  T value;
  is.read(reinterpret_cast<char*>(&value), sizeof(T));
  return value;
}

template<typename T>
static inline void write(std::ostream& os, T value) {
  os.write(reinterpret_cast<const char*>(&value), sizeof(T));
}

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, bool H, typename C, typename A>
template<typename S, typename TT, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t req_compactor<T, H, C, A>::get_serialized_size_bytes(const S&) const {
  return sizeof(state_) + sizeof(section_size_raw_) + sizeof(lg_weight_) + sizeof(num_sections_) +
      sizeof(uint16_t) + // padding
      sizeof(uint32_t) + // num_items
      sizeof(TT) * items_.size();
}

// implementation for all other types
template<typename T, bool H, typename C, typename A>
template<typename S, typename TT, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t req_compactor<T, H, C, A>::get_serialized_size_bytes(const S& serde) const {
  size_t size = sizeof(state_) + sizeof(section_size_raw_) + sizeof(lg_weight_) + sizeof(num_sections_) +
      sizeof(uint16_t) + // padding
      sizeof(uint32_t); // num_items
      sizeof(TT) * items_.size();
  for (const auto& item: items_) size += serde.size_of_item(item);
  return size;
}

template<typename T, bool H, typename C, typename A>
template<typename S>
void req_compactor<T, H, C, A>::serialize(std::ostream& os, const S& serde) const {
  const uint32_t num_items = items_.size();
  write(os, state_);
  write(os, section_size_raw_);
  write(os, lg_weight_);
  write(os, num_sections_);
  const uint16_t padding = 0;
  write(os, padding);
  write(os, num_items);
  serde.serialize(os, items_.data(), items_.size());
}

template<typename T, bool H, typename C, typename A>
template<typename S>
size_t req_compactor<T, H, C, A>::serialize(void* dst, size_t capacity, const S& serde) const {
  uint8_t* ptr = static_cast<uint8_t*>(dst);
  const uint8_t* end_ptr = ptr + capacity;
  ptr += copy_to_mem(state_, ptr);
  ptr += copy_to_mem(section_size_raw_, ptr);
  ptr += copy_to_mem(lg_weight_, ptr);
  ptr += copy_to_mem(num_sections_, ptr);
  const uint16_t padding = 0;
  ptr += copy_to_mem(padding, ptr);
  const uint32_t num_items = items_.size();
  ptr += copy_to_mem(num_items, ptr);
  ptr += serde.serialize(ptr, end_ptr - ptr, items_.data(), items_.size());
  return ptr - static_cast<uint8_t*>(dst);
}

template<typename T, bool H, typename C, typename A>
template<typename S>
req_compactor<T, H, C, A> req_compactor<T, H, C, A>::deserialize(std::istream& is, const S& serde, const A& allocator, bool sorted) {
  auto state = read<decltype(state_)>(is);
  auto section_size_raw = read<decltype(section_size_raw_)>(is);
  auto lg_weight = read<decltype(lg_weight_)>(is);
  auto num_sections = read<decltype(num_sections_)>(is);
  read<uint16_t>(is); // padding
  auto num_items = read<uint32_t>(is);
  auto items = deserialize_items(is, serde, allocator, num_items);
  return req_compactor(lg_weight, sorted, section_size_raw, num_sections, state, std::move(items));
}

template<typename T, bool H, typename C, typename A>
template<typename S>
req_compactor<T, H, C, A> req_compactor<T, H, C, A>::deserialize(std::istream& is, const S& serde, const A& allocator, bool sorted, uint16_t k, uint8_t num_items) {
  auto items = deserialize_items(is, serde, allocator, num_items);
  return req_compactor(0, sorted, k, req_constants::INIT_NUM_SECTIONS, 0, std::move(items));
}

template<typename T, bool H, typename C, typename A>
template<typename S>
std::vector<T, A> req_compactor<T, H, C, A>::deserialize_items(std::istream& is, const S& serde, const A& allocator, size_t num) {
  std::vector<T, A> items(allocator);
  items.reserve(num);
  A alloc(allocator);
  auto item_buffer_deleter = [&alloc](T* ptr) { alloc.deallocate(ptr, 1); };
  std::unique_ptr<T, decltype(item_buffer_deleter)> item_buffer(alloc.allocate(1), item_buffer_deleter);
  for (uint32_t i = 0; i < num; ++i) {
    serde.deserialize(is, item_buffer.get(), 1);
    items.push_back(std::move(*item_buffer));
    (*item_buffer).~T();
  }
  if (!is.good()) throw std::runtime_error("error reading from std::istream");
  return items;
}

template<typename T, bool H, typename C, typename A>
template<typename S>
std::pair<req_compactor<T, H, C, A>, size_t> req_compactor<T, H, C, A>::deserialize(const void* bytes, size_t size, const S& serde, const A& allocator, bool sorted) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  const char* end_ptr = static_cast<const char*>(bytes) + size;

  uint64_t state;
  ptr += copy_from_mem(ptr, state);
  float section_size_raw;
  ptr += copy_from_mem(ptr, section_size_raw);
  uint8_t lg_weight;
  ptr += copy_from_mem(ptr, lg_weight);
  uint8_t num_sections;
  ptr += copy_from_mem(ptr, num_sections);
  ptr += 2; // padding
  uint32_t num_items;
  ptr += copy_from_mem(ptr, num_items);
  auto pair = deserialize_items(ptr, end_ptr - ptr, serde, allocator, num_items);
  ptr += pair.second;
  return std::pair<req_compactor, size_t>(
      req_compactor(lg_weight, sorted, section_size_raw, num_sections, state, std::move(pair.first)),
      ptr - static_cast<const char*>(bytes)
  );
}

template<typename T, bool H, typename C, typename A>
template<typename S>
std::pair<req_compactor<T, H, C, A>, size_t> req_compactor<T, H, C, A>::deserialize(const void* bytes, size_t size, const S& serde, const A& allocator, bool sorted, uint16_t k, uint8_t num_items) {
  auto pair = deserialize_items(bytes, size, serde, allocator, num_items);
  return std::pair<req_compactor, size_t>(
      req_compactor(0, sorted, k, req_constants::INIT_NUM_SECTIONS, 0, std::move(pair.first)),
      pair.second
  );
}

template<typename T, bool H, typename C, typename A>
template<typename S>
std::pair<std::vector<T, A>, size_t> req_compactor<T, H, C, A>::deserialize_items(const void* bytes, size_t size, const S& serde, const A& allocator, size_t num) {
  const char* ptr = static_cast<const char*>(bytes);
  const char* end_ptr = static_cast<const char*>(bytes) + size;
  std::vector<T, A> items(allocator);
  items.reserve(num);
  A alloc(allocator);
  auto item_buffer_deleter = [&alloc](T* ptr) { alloc.deallocate(ptr, 1); };
  std::unique_ptr<T, decltype(item_buffer_deleter)> item_buffer(alloc.allocate(1), item_buffer_deleter);
  for (uint32_t i = 0; i < num; ++i) {
    ptr += serde.deserialize(ptr, end_ptr - ptr, item_buffer.get(), 1);
    items.push_back(std::move(*item_buffer));
    (*item_buffer).~T();
  }
  return std::pair<std::vector<T, A>, size_t>(
      std::move(items),
      ptr - static_cast<const char*>(bytes)
  );
}

template<typename T, bool H, typename C, typename A>
req_compactor<T, H, C, A>::req_compactor(uint8_t lg_weight, bool sorted, float section_size_raw, uint8_t num_sections, uint64_t state, std::vector<T, A>&& items):
lg_weight_(lg_weight),
coin_(req_random_bit()),
sorted_(sorted),
section_size_raw_(section_size_raw),
section_size_(nearest_even(section_size_raw)),
num_sections_(num_sections),
state_(state),
items_(std::move(items))
{}

} /* namespace datasketches */

#endif
