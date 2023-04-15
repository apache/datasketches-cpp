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

#ifndef DENSITY_SKETCH_IMPL_HPP_
#define DENSITY_SKETCH_IMPL_HPP_

#include <algorithm>
#include <sstream>

#include "memory_operations.hpp"
#include "conditional_forward.hpp"

namespace datasketches {

template<typename T, typename K, typename A>
density_sketch<T, K, A>::density_sketch(uint16_t k, uint32_t dim, const K& kernel, const A& allocator):
kernel_(kernel),
k_(k),
dim_(dim),
num_retained_(0),
n_(0),
levels_(1, Level(allocator), allocator)
{}

template<typename T, typename K, typename A>
uint16_t density_sketch<T, K, A>::get_k() const {
  return k_;
}

template<typename T, typename K, typename A>
uint32_t density_sketch<T, K, A>::get_dim() const {
  return dim_;
}

template<typename T, typename K, typename A>
bool density_sketch<T, K, A>::is_empty() const {
  return num_retained_ == 0;
}

template<typename T, typename K, typename A>
uint64_t density_sketch<T, K, A>::get_n() const {
  return n_;
}

template<typename T, typename K, typename A>
uint32_t density_sketch<T, K, A>::get_num_retained() const {
  return num_retained_;
}

template<typename T, typename K, typename A>
bool density_sketch<T, K, A>::is_estimation_mode() const {
  return levels_.size() > 1;
}

template<typename T, typename K, typename A>
template<typename FwdVector>
void density_sketch<T, K, A>::update(FwdVector&& point) {
  if (point.size() != dim_) throw std::invalid_argument("dimension mismatch");
  while (num_retained_ >= k_ * levels_.size()) compact();
  levels_[0].push_back(std::forward<FwdVector>(point));
  ++num_retained_;
  ++n_;
}

template<typename T, typename K, typename A>
template<typename FwdSketch>
void density_sketch<T, K, A>::merge(FwdSketch&& other) {
  if (other.is_empty()) return;
  if (other.dim_ != dim_) throw std::invalid_argument("dimension mismatch");
  while (levels_.size() < other.levels_.size()) levels_.push_back(Level(levels_.get_allocator()));
  for (unsigned height = 0; height < other.levels_.size(); ++height) {
    std::copy(
      forward_begin(conditional_forward<FwdSketch>(other.levels_[height])),
      forward_end(conditional_forward<FwdSketch>(other.levels_[height])),
      back_inserter(levels_[height])
    );
  }
  num_retained_ += other.num_retained_;
  n_ += other.n_;
  while (num_retained_ >= k_ * levels_.size()) compact();
}

template<typename T, typename K, typename A>
T density_sketch<T, K, A>::get_estimate(const std::vector<T>& point) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  T density = 0;
  for (unsigned height = 0; height < levels_.size(); ++height) {
    for (const auto& p: levels_[height]) {
      density += (1 << height) * kernel_(p, point) / n_;
    }
  }
  return density;
}

template<typename T, typename K, typename A>
A density_sketch<T, K, A>::get_allocator() const {
  return levels_.get_allocator();
}

template<typename T, typename K, typename A>
void density_sketch<T, K, A>::compact() {
  for (unsigned height = 0; height < levels_.size(); ++height) {
    if (levels_[height].size() >= k_) {
      if (height + 1 >= levels_.size()) levels_.push_back(Level(levels_.get_allocator()));
      compact_level(height);
      break;
    }
  }
}

template<typename T, typename K, typename A>
void density_sketch<T, K, A>::compact_level(unsigned height) {
  auto& level = levels_[height];
  std::vector<bool> bits(level.size());
  bits[0] = random_bit();
  std::random_shuffle(level.begin(), level.end());
  for (unsigned i = 1; i < level.size(); ++i) {
    T delta = 0;
    for (unsigned j = 0; j < i; ++j) {
      delta += (bits[j] ? 1 : -1) * kernel_(level[i], level[j]);
    }
    bits[i] = delta < 0;
  }
  for (unsigned i = 0; i < level.size(); ++i) {
    if (bits[i]) {
      levels_[height + 1].push_back(std::move(level[i]));
    } else {
      --num_retained_;
    }
  }
  level.clear();
}

/* Serialized sketch layout:
 * Int  || Start Byte Addr:
 * Addr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Ints  | SerVer | FamID  |  Flags |------- K -------|---- unused -----|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  2   ||---------------------------Items Seen Count (N)--------------------------------|
 *
 *      ||       16       |    17   |   18   |   19   |   12   |   13   |   14   |   15  |
 *  4   ||------------- Num Dimensions ---------------|----- Start of Levels Data -------|
 *
 * Ints 2 and 3 are omitted when the sketch is empty, meaning Num Dimensions is stored at
 * offset 8 in that case. Otherwise, Int 5 is the start of level data, consisting of the
 * size of the level (as a uint32 value) followed by that number of points, with 
 * Num Dimensions per point.
 */

template<typename T, typename K, typename A>
void density_sketch<T, K, A>::serialize(std::ostream& os) const {
  const uint8_t preamble_ints = is_empty() ? PREAMBLE_INTS_SHORT : PREAMBLE_INTS_LONG;
  write(os, preamble_ints);
  const uint8_t ser_ver = SERIAL_VERSION;
  write(os, ser_ver);
  const uint8_t family = FAMILY_ID;
  write(os, family);

  // only empty is a valid flag
  const uint8_t flags_byte = (is_empty() ? 1 << flags::IS_EMPTY : 0);
  write(os, flags_byte);
  write(os, k_);
  const uint16_t unused = 0;
  write(os, unused);

  if (!is_empty())
    write(os, n_);

  write(os, dim_);

  // levels array -- uint32_t since a single level may be larger than k
  size_t pt_size = sizeof(T) * dim_;
  for (Level lvl : levels_) {
    const uint32_t level_size = static_cast<uint32_t>(lvl.size());
    write(os, level_size);
    for (Vector pt : lvl) {
      write(os, &pt.data(), pt_size);
    }
  }
}

template<typename T, typename K, typename A>
auto density_sketch<T, K, A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  const uint8_t preamble_ints = (is_empty() ? PREAMBLE_INTS_SHORT : PREAMBLE_INTS_LONG);
  
  // pre-compute size
  const size_t size = header_size_bytes + preamble_ints;
  for (Level lvl : levels_) {
    size += sizeof(uint32_t) + (lvl.size() * dim_ * sizeof(T));
  }

  vector_bytes bytes(size, 0, levels_.get_allocator());
  uint8_t* ptr = bytes.data() + header_size_bytes;
  const uint8_t* end_ptr = ptr + size;
  
  ptr += copy_to_mem(preamble_ints, ptr);
  const uint8_t ser_ver = SERIAL_VERSION;
  ptr += copy_to_mem(ser_ver, ptr);
  const uint8_t family = FAMILY_ID;
  ptr += copy_to_mem(family, ptr);

  // empty is the only valid flat
  const uint8_t flags_byte = (is_empty() ? 1 << flags::IS_EMPTY : 0);
  ptr += copy_to_mem(flags_byte, ptr);
  ptr += copy_to_mem(k_, ptr);
  ptr += sizeof(uint16_t); // 2 unused bytes
  
  if (!is_empty())
    ptr += copy_to_mem(n_, ptr);
  
  ptr += copy_to_mem(dim_, ptr);

  // levels array -- uint32_t since a single level may be larger than k
  size_t pt_size = sizeof(T) * dim_;
  for (Level lvl : levels_) {
    ptr += copy_to_mem(static_cast<uint32_t>(lvl.size()), ptr);
    for (Vector pt : lvl) {
      ptr += copy_to_mem(&pt.data(), ptr, pt_size);
    }
  }

  if (ptr != end_ptr)
    throw std::runtime_error("Actual output size does not equal expected output size");

  return bytes;
}

template<typename T, typename K, typename A>
density_sketch<T, K, A> density_sketch<T, K, A>::deserialize(std::istream& is, K& kernel, const A& allocator) {

}

template<typename T, typename K, typename A>
density_sketch<T, K, A> density_sketch<T, K, A>::deserialize(const void* bytes, size_t size, K& kernel, const A& allocator) {

}


template<typename T, typename K, typename A>
string<A> density_sketch<T, K, A>::to_string(bool print_levels, bool print_items) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Density sketch summary:" << std::endl;
  os << "   K              : " << k_ << std::endl;
  os << "   Dim            : " << dim_ << std::endl;
  os << "   Empty          : " << (is_empty() ? "true" : "false") << std::endl;
  os << "   N              : " << n_ << std::endl;
  os << "   Retained items : " << num_retained_ << std::endl;
  os << "   Estimation mode: " << (is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   Levels         : " << levels_.size() << std::endl;
  os << "### End sketch summary" << std::endl;

  if (print_levels) {
    os << "### Density sketch levels:" << std::endl;
    os << "   height: size" << std::endl;
    for (unsigned height = 0; height < levels_.size(); ++height) {
      os << "   " << height << ": "
        << levels_[height].size() << std::endl;
    }
    os << "### End sketch levels" << std::endl;
  }

  if (print_items) {
    os << "### Density sketch data:" << std::endl;
    unsigned level = 0;
    for (unsigned height = 0; height < levels_.size(); ++height) {
      os << " level " << height << ": " << std::endl;
      for (const auto& point: levels_[height]) {
        os << "   [";
        bool first = true;
        for (auto value: point) {
          if (first) {
            first = false;
          } else {
            os << ", ";
          }
          os << value;
        }
        os << "]" << std::endl;
      }
      ++level;
    }
    os << "### End sketch data" << std::endl;
  }
  return string<A>(os.str().c_str(), levels_.get_allocator());
}

template<typename T, typename K, typename A>
auto density_sketch<T, K, A>::begin() const -> const_iterator {
  return const_iterator(levels_.begin(), levels_.end());
}

template<typename T, typename K, typename A>
auto density_sketch<T, K, A>::end() const -> const_iterator {
  return const_iterator(levels_.end(), levels_.end());
}

// iterator

template<typename T, typename K, typename A>
density_sketch<T, K, A>::const_iterator::const_iterator(LevelsIterator begin, LevelsIterator end):
levels_it_(begin),
levels_end_(end),
level_it_(),
height_(0)
{
  // skip empty levels
  while (levels_it_ != levels_end_) {
    level_it_ = levels_it_->begin();
    if (level_it_ != levels_it_->end()) break;
    ++levels_it_;
    ++height_;
  }
}

template<typename T, typename K, typename A>
auto density_sketch<T, K, A>::const_iterator::operator++() -> const_iterator& {
  ++level_it_;
  if (level_it_ == levels_it_->end()) {
    ++levels_it_;
    ++height_;
    // skip empty levels
    while (levels_it_ != levels_end_) {
      level_it_ = levels_it_->begin();
      if (level_it_ != levels_it_->end()) break;
      ++levels_it_;
      ++height_;
    }
  }
  return *this;
}

template<typename T, typename K, typename A>
auto density_sketch<T, K, A>::const_iterator::operator++(int) -> const_iterator& {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename K, typename A>
bool density_sketch<T, K, A>::const_iterator::operator==(const const_iterator& other) const {
  if (levels_it_ != other.levels_it_) return false;
  if (levels_it_ == levels_end_) return true;
  return level_it_ == other.level_it_;
}

template<typename T, typename K, typename A>
bool density_sketch<T, K, A>::const_iterator::operator!=(const const_iterator& other) const {
  return !operator==(other);
}

template<typename T, typename K, typename A>
auto density_sketch<T, K, A>::const_iterator::operator*() const -> const value_type {
  return value_type(*level_it_, 1ULL << height_);
}

template<typename T, typename K, typename A>
auto density_sketch<T, K, A>::const_iterator::operator->() const -> const return_value_holder<value_type> {
  return **this;
}

} /* namespace datasketches */

#endif
