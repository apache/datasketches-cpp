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

#include <iostream>
#include <sstream>
#include <algorithm>

namespace datasketches {

template<typename EN, typename EK, typename A>
theta_update_sketch_base<EN, EK, A>::theta_update_sketch_base(uint8_t lg_cur_size, uint8_t lg_nom_size, resize_factor rf, float p, uint64_t seed):
is_empty_(true),
lg_cur_size_(lg_cur_size),
lg_nom_size_(lg_nom_size),
rf_(rf),
p_(p),
num_entries_(0),
theta_(theta_constants::MAX_THETA),
seed_(seed),
entries_(nullptr)
{
  const size_t size = 1 << lg_cur_size;
  entries_ = A().allocate(size);
  for (size_t i = 0; i < size; ++i) EK()(entries_[i]) = 0;
  if (p < 1) this->theta_ *= p;
}

template<typename EN, typename EK, typename A>
theta_update_sketch_base<EN, EK, A>::~theta_update_sketch_base()
{
  const size_t size = 1 << lg_cur_size_;
  for (size_t i = 0; i < size; ++i) {
    if (EK()(entries_[i]) != 0) entries_[i].~EN();
  }
  A().deallocate(entries_, size);
}

//template<typename EN, typename EK, typename A>
//void theta_base<EN, EK, A>::update(EN&& entry) {
//  const uint64_t hash = EK()(entry);
//  if (hash >= this->theta_ || hash == 0) return; // hash == 0 is reserved to mark empty slots in the table
//  auto result = find(hash);
//  if (!result.second) {
//    insert(result.first, std::forward<EN>(entry));
//  }
//}

template<typename EN, typename EK, typename A>
auto theta_update_sketch_base<EN, EK, A>::find(uint64_t key) const -> std::pair<iterator, bool> {
  const size_t size = 1 << lg_cur_size_;
  const size_t mask = size - 1;
  const uint32_t stride = get_stride(key, lg_cur_size_);
  uint32_t index = static_cast<uint32_t>(key) & mask;
  // search for duplicate or zero
  const uint32_t loop_index = index;
  do {
    const uint64_t probe = EK()(entries_[index]);
    if (probe == 0) {
      return std::pair<iterator, bool>(&entries_[index], false);
    } else if (probe == key) {
      return std::pair<iterator, bool>(&entries_[index], true);
    }
    index = (index + stride) & mask;
  } while (index != loop_index);
  throw std::logic_error("key not found and no empty slots!");
}

template<typename EN, typename EK, typename A>
template<typename Fwd>
void theta_update_sketch_base<EN, EK, A>::insert(iterator it, Fwd&& entry) {
  new (it) EN(std::forward<Fwd>(entry));
  is_empty_ = false;
  ++num_entries_;
  if (num_entries_ > get_capacity(lg_cur_size_, lg_nom_size_)) {
    if (lg_cur_size_ <= lg_nom_size_) {
      resize();
    } else {
      rebuild();
    }
  }
}

template<typename EN, typename EK, typename A>
auto theta_update_sketch_base<EN, EK, A>::begin() const -> iterator {
  return entries_;
}

template<typename EN, typename EK, typename A>
auto theta_update_sketch_base<EN, EK, A>::end() const -> iterator {
  return &entries_[1 << lg_cur_size_];
}

template<typename EN, typename EK, typename A>
string<A> theta_update_sketch_base<EN, EK, A>::to_string() const {
  std::basic_ostringstream<char, std::char_traits<char>, AllocChar<A>> os;
  auto type = typeid(*this).name();
  os << "type: " << type << std::endl;
  os << "sizeof: " << sizeof(*this) << std::endl;
  os << "is_empty:    " << (is_empty_ ? "true" : "false") << std::endl;
  os << "lg_cur_size: " << std::to_string(lg_cur_size_) << std::endl;
  os << "lg_nom_size: " << std::to_string(lg_nom_size_) << std::endl;
  os << "num_entries: " << num_entries_ << std::endl;
  os << "theta (as long): " << theta_ << std::endl;
  os << "theta (as fraction): " << static_cast<double>(theta_) / theta_constants::MAX_THETA << std::endl;
  return os.str();
}

template<typename EN, typename EK, typename A>
uint32_t theta_update_sketch_base<EN, EK, A>::get_capacity(uint8_t lg_cur_size, uint8_t lg_nom_size) {
  const double fraction = (lg_cur_size <= lg_nom_size) ? RESIZE_THRESHOLD : REBUILD_THRESHOLD;
  return std::floor(fraction * (1 << lg_cur_size));
}

template<typename EN, typename EK, typename A>
uint32_t theta_update_sketch_base<EN, EK, A>::get_stride(uint64_t key, uint8_t lg_size) {
  // odd and independent of index assuming lg_size lowest bits of the key were used for the index
  return (2 * static_cast<uint32_t>((key >> lg_size) & STRIDE_MASK)) + 1;
}

template<typename EN, typename EK, typename A>
void theta_update_sketch_base<EN, EK, A>::resize() {
  const size_t old_size = 1 << lg_cur_size_;
  const uint8_t lg_tgt_size = lg_nom_size_ + 1;
  const uint8_t factor = std::max(1, std::min(static_cast<int>(rf_), lg_tgt_size - lg_cur_size_));
  lg_cur_size_ += factor;
  const size_t new_size = 1 << lg_cur_size_;
  EN* old_entries = entries_;
  entries_ = A().allocate(new_size);
  for (size_t i = 0; i < new_size; ++i) EK()(entries_[i]) = 0;
  num_entries_ = 0;
  for (size_t i = 0; i < old_size; ++i) {
    const uint64_t key = EK()(old_entries[i]);
    if (key != 0) {
      insert(find(key).first, std::move(old_entries[i])); // consider a special insert with no comparison
      old_entries[i].~EN();
    }
  }
  A().deallocate(old_entries, old_size);
}

template<typename EN, typename EK, typename A>
void theta_update_sketch_base<EN, EK, A>::rebuild() {
  const size_t size = 1 << lg_cur_size_;
  const uint32_t pivot = (1 << lg_nom_size_) + size - num_entries_;
  std::nth_element(&entries_[0], &entries_[pivot], &entries_[size], comparator());
  this->theta_ = EK()(entries_[pivot]);
  EN* old_entries = entries_;
  entries_ = A().allocate(size);
  for (size_t i = 0; i < size; ++i) EK()(entries_[i]) = 0;
  num_entries_ = 0;
  for (size_t i = 0; i < size; ++i) {
    const uint64_t key = EK()(old_entries[i]);
    if (key != 0 && key < this->theta_) {
      insert(find(key).first, std::move(old_entries[i])); // consider a special insert with no comparison
      old_entries[i].~EN();
    }
  }
  A().deallocate(old_entries, size);
}

template<typename EN, typename EK, typename A>
void theta_update_sketch_base<EN, EK, A>::trim() {
  if (num_entries_ > static_cast<uint32_t>(1 << lg_nom_size_)) rebuild();
}

// builder

template<bool dummy>
theta_base_builder<dummy>::theta_base_builder():
lg_k_(DEFAULT_LG_K), rf_(DEFAULT_RESIZE_FACTOR), p_(1), seed_(DEFAULT_SEED) {}

template<bool dummy>
auto theta_base_builder<dummy>::set_lg_k(uint8_t lg_k) -> theta_base_builder& {
  if (lg_k < MIN_LG_K) {
    throw std::invalid_argument("lg_k must not be less than " + std::to_string(MIN_LG_K) + ": " + std::to_string(lg_k));
  }
  lg_k_ = lg_k;
  return *this;
}

template<bool dummy>
auto theta_base_builder<dummy>::set_resize_factor(resize_factor rf) -> theta_base_builder& {
  rf_ = rf;
  return *this;
}

template<bool dummy>
auto theta_base_builder<dummy>::set_p(float p) -> theta_base_builder& {
  if (p < 0 || p > 1) throw std::invalid_argument("sampling probability must be between 0 and 1");
  p_ = p;
  return *this;
}

template<bool dummy>
auto theta_base_builder<dummy>::set_seed(uint64_t seed) -> theta_base_builder& {
  seed_ = seed;
  return *this;
}

template<bool dummy>
uint8_t theta_base_builder<dummy>::starting_sub_multiple(uint8_t lg_tgt, uint8_t lg_min, uint8_t lg_rf) {
  return (lg_tgt <= lg_min) ? lg_min : (lg_rf == 0) ? lg_tgt : ((lg_tgt - lg_min) % lg_rf) + lg_min;
}

// iterator

template<typename Entry, typename ExtractKey>
theta_const_iterator<Entry, ExtractKey>::theta_const_iterator(const Entry* entries, uint32_t size, uint32_t index):
entries_(entries), size_(size), index_(index) {
  while (index_ < size_ && ExtractKey()(entries_[index_]) == 0) ++index_;
}

template<typename Entry, typename ExtractKey>
auto theta_const_iterator<Entry, ExtractKey>::operator++() -> theta_const_iterator& {
  ++index_;
  while (index_ < size_ && ExtractKey()(entries_[index_]) == 0) ++index_;
  return *this;
}

template<typename Entry, typename ExtractKey>
auto theta_const_iterator<Entry, ExtractKey>::operator++(int) -> theta_const_iterator {
  theta_const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename Entry, typename ExtractKey>
bool theta_const_iterator<Entry, ExtractKey>::operator!=(const theta_const_iterator& other) const {
  return index_ != other.index_;
}

template<typename Entry, typename ExtractKey>
bool theta_const_iterator<Entry, ExtractKey>::operator==(const theta_const_iterator& other) const {
  return index_ == other.index_;
}

template<typename Entry, typename ExtractKey>
auto theta_const_iterator<Entry, ExtractKey>::operator*() const -> const Entry& {
  return entries_[index_];
}

} /* namespace datasketches */
