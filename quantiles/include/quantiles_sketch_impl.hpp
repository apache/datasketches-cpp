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

#ifndef _QUANTILES_SKETCH_IMPL_HPP_
#define _QUANTILES_SKETCH_IMPL_HPP_

#include <cmath>
#include <algorithm>
#include <stdexcept>
#include <iomanip>

#include "common_defs.hpp"
#include "count_zeros.hpp"
#include "quantiles_sketch.hpp"

namespace datasketches {

template<typename T, typename C, typename S, typename A>
quantiles_sketch<T, C, S, A>::quantiles_sketch(uint16_t k, const A& allocator):
allocator_(allocator),
k_(k),
n_(0),
bit_pattern_(0),
base_buffer_(allocator_),
levels_(allocator_),
min_value_(nullptr),
max_value_(nullptr),
is_sorted_(true)
{
  if (k < quantiles_constants::MIN_K || k > quantiles_constants::MAX_K) {
    throw std::invalid_argument("K must be >= " + std::to_string(quantiles_constants::MIN_K) + " and <= " + std::to_string(quantiles_constants::MAX_K) + ": " + std::to_string(k));
  }
  base_buffer_.reserve(2 * std::min(quantiles_constants::MIN_K, k));
}

template<typename T, typename C, typename S, typename A>
quantiles_sketch<T, C, S, A>::quantiles_sketch(const quantiles_sketch& other):
allocator_(other.allocator_),
k_(other.k_),
n_(other.n_),
bit_pattern_(other.bit_pattern_),
base_buffer_(other.base_buffer_),
levels_(other.levels_),
min_value_(nullptr),
max_value_(nullptr),
is_sorted_(other.is_sorted_)
{
  if (other.min_value_ != nullptr) min_value_ = new (allocator_.allocate(1)) T(*other.min_value_);
  if (other.max_value_ != nullptr) max_value_ = new (allocator_.allocate(1)) T(*other.max_value_);
}

template<typename T, typename C, typename S, typename A>
quantiles_sketch<T, C, S, A>::quantiles_sketch(quantiles_sketch&& other) noexcept:
allocator_(other.allocator_),
k_(other.k_),
n_(other.n_),
bit_pattern_(other.bit_pattern_),
base_buffer_(std::move(other.base_buffer_)),
levels_(std::move(other.levels_)),
min_value_(other.min_value_),
max_value_(other.max_value_),
is_sorted_(other.is_sorted_)
{
  other.min_value_ = nullptr;
  other.max_value_ = nullptr;
}

template<typename T, typename C, typename S, typename A>
quantiles_sketch<T, C, S, A>& quantiles_sketch<T, C, S, A>::operator=(const quantiles_sketch& other) {
  quantiles_sketch<T, C, S, A> copy(other);
  std::swap(allocator_, copy.allocator_);
  std::swap(k_, copy.k_);
  std::swap(n_, copy.n_);
  std::swap(bit_pattern_, copy.bit_pattern_);
  std::swap(base_buffer_, copy.base_buffer_);
  std::swap(levels_, copy.levels_);
  std::swap(min_value_, copy.min_value_);
  std::swap(max_value_, copy.max_value_);
  std::swap(is_sorted_, copy.is_sorted_);
  return *this;
}

template<typename T, typename C, typename S, typename A>
quantiles_sketch<T, C, S, A>& quantiles_sketch<T, C, S, A>::operator=(quantiles_sketch&& other) noexcept {
  std::swap(allocator_, other.allocator_);
  std::swap(k_, other.k_);
  std::swap(n_, other.n_);
  std::swap(bit_pattern_, other.bit_pattern_);
  std::swap(base_buffer_, other.base_buffer_);
  std::swap(levels_, other.levels_);
  std::swap(min_value_, other.min_value_);
  std::swap(max_value_, other.max_value_);
  std::swap(is_sorted_, other.is_sorted_);
  return *this;
}


template<typename T, typename C, typename S, typename A>
quantiles_sketch<T, C, S, A>::~quantiles_sketch() {
  if (min_value_ != nullptr) {
    min_value_->~T();
    allocator_.deallocate(min_value_, 1);
  }
  if (max_value_ != nullptr) {
    max_value_->~T();
    allocator_.deallocate(max_value_, 1);
  }
}

template<typename T, typename C, typename S, typename A>
template<typename FwdT>
void quantiles_sketch<T, C, S, A>::update(FwdT&& item) {
  if (!check_update_value(item)) { return; }
  if (is_empty()) {
    min_value_ = new (allocator_.allocate(1)) T(item);
    max_value_ = new (allocator_.allocate(1)) T(item);
  } else {
    if (C()(item, *min_value_)) *min_value_ = item;
    if (C()(*max_value_, item)) *max_value_ = item;
  }

  // if exceed capacity, grow until size 2k -- assumes eager processing
  if (base_buffer_.size() + 1 > base_buffer_.capacity())
    grow_base_buffer();

  base_buffer_.push_back(std::forward<FwdT>(item));
  ++n_;

  if (base_buffer_.size() > 1)
    is_sorted_ = false;
  
  if (base_buffer_.size() == 2 * k_)
    process_full_base_buffer();
}

template<typename T, typename C, typename S, typename A>
void quantiles_sketch<T, C, S, A>::serialize(std::ostream& os) const {
  const uint8_t preamble_longs = is_empty() ? PREAMBLE_LONGS_SHORT : PREAMBLE_LONGS_FULL;
  write(os, preamble_longs);
  const uint8_t ser_ver = SERIAL_VERSION_3;
  write(os, ser_ver);
  const uint8_t family = FAMILY;
  write(os, family);

  // empty, ordered, compact are valid flags
  const uint8_t flags_byte(
      (is_empty() ? 1 << flags::IS_EMPTY : 0)
    | (is_sorted_ ? 1 << flags::IS_SORTED : 0)
    | (1 << flags::IS_COMPACT) // always compact -- could be optional for numeric types?
  );
  write(os, flags_byte);
  write(os, k_);
  uint16_t unused = 0;
  write(os, unused);

  if (!is_empty()) {
    write(os, n_);

    // min and max
    S().serialize(os, min_value_, 1);
    S().serialize(os, max_value_, 1);

    // base buffer items
    S().serialize(os, base_buffer_.data(), base_buffer_.size());

    // levels, only when data is present
    for (Level lvl : levels_) {
      if (lvl.size() > 0)
        S().serialize(os, lvl.data(), lvl.size());
    }
  }
}

template<typename T, typename C, typename S, typename A>
auto quantiles_sketch<T, C, S, A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  const size_t size = get_serialized_size_bytes() + header_size_bytes;
  vector_bytes bytes(size, 0, allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;
  const uint8_t* end_ptr = ptr + size;
  
  const uint8_t preamble_longs = is_empty() ? PREAMBLE_LONGS_SHORT : PREAMBLE_LONGS_FULL;
  ptr += copy_to_mem(preamble_longs, ptr);
  const uint8_t ser_ver = SERIAL_VERSION_3;
  ptr += copy_to_mem(ser_ver, ptr);
  const uint8_t family = FAMILY;
  ptr += copy_to_mem(family, ptr);

  // empty, ordered, compact are valid flags
  const uint8_t flags_byte(
      (is_empty() ? 1 << flags::IS_EMPTY : 0)
    | (is_sorted_ ? 1 << flags::IS_SORTED : 0)
    | (1 << flags::IS_COMPACT) // always compact -- could be optional for numeric types?
  );
  ptr += copy_to_mem(flags_byte, ptr);
  ptr += copy_to_mem(k_, ptr);
  ptr += sizeof(uint16_t); // 2 unused bytes
  
  if (!is_empty()) {
    ptr += copy_to_mem(n_, ptr);
    
    // min and max
    ptr += S().serialize(ptr, end_ptr - ptr, min_value_, 1);
    ptr += S().serialize(ptr, end_ptr - ptr, max_value_, 1);
 
    // base buffer items
    if (base_buffer_.size() > 0)
      ptr += S().serialize(ptr, end_ptr - ptr, base_buffer_.data(), base_buffer_.size());
    
    // levels, only when data is present
    for (Level lvl : levels_) {
      if (lvl.size() > 0)
        ptr += S().serialize(ptr, end_ptr - ptr, lvl.data(), lvl.size());
    }
  }

  return bytes;
}


template<typename T, typename C, typename S, typename A>
string<A> quantiles_sketch<T, C, S, A>::to_string(bool print_levels, bool print_items) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### Quantiles Sketch summary:" << std::endl;
  os << "   K              : " << k_ << std::endl;
  os << "   N              : " << n_ << std::endl;
  os << "   Epsilon        : " << std::setprecision(3) << get_normalized_rank_error(false) * 100 << "%" << std::endl;
  os << "   Epsilon PMF    : " << get_normalized_rank_error(true) * 100 << "%" << std::endl;
  os << "   Empty          : " << (is_empty() ? "true" : "false") << std::endl;
  os << "   Estimation mode: " << (is_estimation_mode() ? "true" : "false") << std::endl;
  os << "   Levels (w/o BB): " << levels_.size() << std::endl;
  os << "   Used Levels    : " << compute_valid_levels(bit_pattern_) << std::endl;
  os << "   Retained items : " << get_num_retained() << std::endl;
  os << "   Storage bytes  : " << get_serialized_size_bytes() << std::endl;
  if (!is_empty()) {
    os << "   Min value      : " << *min_value_ << std::endl;
    os << "   Max value      : " << *max_value_ << std::endl;
  }
  os << "### End sketch summary" << std::endl;

  if (print_levels) {
    os << "### Quantiles Sketch levels:" << std::endl;
    os << "   index: items in use" << std::endl;
    os << "   BB: " << base_buffer_.size() << std::endl;
    for (uint8_t i = 0; i < levels_.size(); i++) {
      os << "   " << static_cast<unsigned int>(i) << ": " << levels_[i].size() << std::endl;
    }
    os << "### End sketch levels" << std::endl;
  }

  if (print_items) {
    os << "### Quantiles Sketch data:" << std::endl;
    uint8_t level = 0;
    os << " BB:" << std::endl;
    for (const T& item : base_buffer_) {
      os << "    " << std::to_string(item) << std::endl;
    }
    for (uint8_t i = 0; i < levels_.size(); ++i) {
      os << " level " << static_cast<unsigned int>(level) << ":" << std::endl;
      for (const T& item : levels_[i]) {
        os << "   " << std::to_string(item) << std::endl;
      }
    }
    os << "### End sketch data" << std::endl;
  }
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename C, typename S, typename A>
uint16_t quantiles_sketch<T, C, S, A>::get_k() const {
  return k_;
}

template<typename T, typename C, typename S, typename A>
uint64_t quantiles_sketch<T, C, S, A>::get_n() const {
  return n_;
}

template<typename T, typename C, typename S, typename A>
bool quantiles_sketch<T, C, S, A>::is_empty() const {
  return n_ == 0;
}

template<typename T, typename C, typename S, typename A>
bool quantiles_sketch<T, C, S, A>::is_estimation_mode() const {
  return bit_pattern_ != 0;
}

template<typename T, typename C, typename S, typename A>
uint32_t quantiles_sketch<T, C, S, A>::get_num_retained() const {
  return compute_retained_items(k_, n_);
}

template<typename T, typename C, typename S, typename A>
const T& quantiles_sketch<T, C, S, A>::get_min_value() const {
  if (is_empty()) return get_invalid_value();
  return *min_value_;
}

template<typename T, typename C, typename S, typename A>
const T& quantiles_sketch<T, C, S, A>::get_max_value() const {
  if (is_empty()) return get_invalid_value();
  return *max_value_;
}

// implementation for fixed-size arithmetic types (integral and floating point)
template<typename T, typename C, typename S, typename A>
template<typename TT, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type>
size_t quantiles_sketch<T, C, S, A>::get_serialized_size_bytes() const {
  if (is_empty()) { return EMPTY_SIZE_BYTES; }
  return DATA_START + ((get_num_retained() + 2) * sizeof(TT));
}

// implementation for all other types
template<typename T, typename C, typename S, typename A>
template<typename TT, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type>
size_t quantiles_sketch<T, C, S, A>::get_serialized_size_bytes() const {
  if (is_empty()) { return EMPTY_SIZE_BYTES; }
  size_t size = DATA_START;
  size += S().size_of_item(*min_value_);
  size += S().size_of_item(*max_value_);
  for (auto it: *this) size += S().size_of_item(it.first);
  return size;
}

template<typename T, typename C, typename S, typename A>
double quantiles_sketch<T, C, S, A>::get_normalized_rank_error(bool is_pmf) const {
  return get_normalized_rank_error(k_, is_pmf);
}

template<typename T, typename C, typename S, typename A>
double quantiles_sketch<T, C, S, A>::get_normalized_rank_error(uint16_t k, bool is_pmf) {
  return is_pmf
      ? 1.854 / std::pow(k, 0.9657)
      : 1.576 / std::pow(k, 0.9726);
}

template<typename T, typename C, typename S, typename A>
class quantiles_sketch<T, C, S, A>::calculator_deleter {
  public:
  calculator_deleter(const AllocCalc& allocator): allocator_(allocator) {}
  void operator() (QuantileCalculator* ptr) {
    if (ptr != nullptr) {
      ptr->~QuantileCalculator();
      allocator_.deallocate(ptr, 1);
    }
  }
  private:
  AllocCalc allocator_;
};

template<typename T, typename C, typename S, typename A>
template<bool inclusive>
auto quantiles_sketch<T, C, S, A>::get_quantile_calculator() const -> QuantileCalculatorPtr {
  // allow side effect of sorting the base buffer
  // can't set the sorted flag since this is a const method
  if (!is_sorted_) {
    std::sort(const_cast<Level&>(base_buffer_).begin(), const_cast<Level&>(base_buffer_).end(), C());
  }

  AllocCalc ac(allocator_);
  QuantileCalculatorPtr quantile_calculator_ptr(
    new (ac.allocate(1)) quantile_calculator<T, C, A>(n_, ac),
    calculator_deleter(ac)
  );

  uint64_t lg_weight = 0;
  quantile_calculator_ptr->add(base_buffer_.data(), base_buffer_.data() + base_buffer_.size(), lg_weight);
  for (auto& level : levels_) {
    ++lg_weight;
    if (level.empty()) { continue; }
    quantile_calculator_ptr->add(level.data(), level.data() + k_, lg_weight);
  }
  quantile_calculator_ptr->template convert_to_cummulative<inclusive>();
  return quantile_calculator_ptr;
}

template<typename T, typename C, typename S, typename A>
template<bool inclusive>
const T& quantiles_sketch<T, C, S, A>::get_quantile(double rank) const {
  if (is_empty()) return get_invalid_value();
  if (rank == 0.0) return *min_value_;
  if (rank == 1.0) return *max_value_;
  if ((rank < 0.0) || (rank > 1.0)) {
    throw std::invalid_argument("Rank cannot be less than zero or greater than 1.0");
  }
  return *(get_quantile_calculator<inclusive>()->get_quantile(rank));
}

template<typename T, typename C, typename S, typename A>
template<bool inclusive>
std::vector<T, A> quantiles_sketch<T, C, S, A>::get_quantiles(const double* ranks, uint32_t size) const {
  std::vector<T, A> quantiles(allocator_);
  if (is_empty()) return quantiles;
  QuantileCalculatorPtr quantile_calculator_ptr(nullptr, calculator_deleter(allocator_));
  quantiles.reserve(size);
  for (uint32_t i = 0; i < size; ++i) {
    const double rank = ranks[i];
    if ((rank < 0.0) || (rank > 1.0)) {
      throw std::invalid_argument("rank cannot be less than zero or greater than 1.0");
    }
    if      (rank == 0.0) quantiles.push_back(*min_value_);
    else if (rank == 1.0) quantiles.push_back(*max_value_);
    else {
      if (!quantile_calculator_ptr) {
        // has side effect of sorting level zero if needed
        quantile_calculator_ptr = const_cast<quantiles_sketch*>(this)->get_quantile_calculator<inclusive>();
      }
      quantiles.push_back(*(quantile_calculator_ptr->get_quantile(rank)));
    }
  }
  return quantiles;
}

template<typename T, typename C, typename S, typename A>
template<bool inclusive>
std::vector<T, A> quantiles_sketch<T, C, S, A>::get_quantiles(uint32_t num) const {
  if (is_empty()) return std::vector<T, A>(allocator_);
  if (num == 0) {
    throw std::invalid_argument("num must be > 0");
  }
  vector_double fractions(num, 0, allocator_);
  fractions[0] = 0.0;
  for (size_t i = 1; i < num; i++) {
    fractions[i] = static_cast<double>(i) / (num - 1);
  }
  if (num > 1) {
    fractions[num - 1] = 1.0;
  }
  return get_quantiles<inclusive>(fractions.data(), num);
}

template<typename T, typename C, typename S, typename A>
template<bool inclusive>
double quantiles_sketch<T, C, S, A>::get_rank(const T& value) const {
  if (is_empty()) return std::numeric_limits<double>::quiet_NaN();
  uint64_t weight = 1;
  uint64_t total = 0;
  for (const T &item: base_buffer_) {
    if (inclusive ? !C()(value, item) : C()(item, value))
      total += weight;
  }

  weight *= 2;
  for (uint8_t level = 0; level < levels_.size(); ++level, weight *= 2) {
    if (levels_[level].empty()) { continue; }
    const T* data = levels_[level].data();
    for (uint16_t i = 0; i < k_; ++i) {
      if (inclusive ? !C()(value, data[i]) : C()(data[i], value))
        total += weight;
      else
        break;  // levels are sorted, no point comparing further
    }
  }
  return (double) total / n_;
}

template<typename T, typename C, typename S, typename A>
template<bool inclusive>
auto quantiles_sketch<T, C, S, A>::get_PMF(const T* split_points, uint32_t size) const -> vector_double {
  auto buckets = get_CDF<inclusive>(split_points, size);
  if (is_empty()) return buckets;
  for (uint32_t i = size; i > 0; --i) {
    buckets[i] -= buckets[i - 1];
  }
  return buckets;
}

template<typename T, typename C, typename S, typename A>
template<bool inclusive>
auto quantiles_sketch<T, C, S, A>::get_CDF(const T* split_points, uint32_t size) const -> vector_double {
  vector_double buckets(allocator_);
  if (is_empty()) return buckets;
  check_split_points(split_points, size);
  buckets.reserve(size + 1);
  for (uint32_t i = 0; i < size; ++i) buckets.push_back(get_rank<inclusive>(split_points[i]));
  buckets.push_back(1);
  return buckets;
}

template<typename T, typename C, typename S, typename A>
uint32_t quantiles_sketch<T, C, S, A>::compute_retained_items(const uint16_t k, const uint64_t n) {
  uint32_t bb_count = compute_base_buffer_items(k, n);
  uint64_t bit_pattern = compute_bit_pattern(k, n);
  uint32_t valid_levels = compute_valid_levels(bit_pattern);
  return bb_count + (k * valid_levels);
}

template<typename T, typename C, typename S, typename A>
uint32_t quantiles_sketch<T, C, S, A>::compute_base_buffer_items(const uint16_t k, const uint64_t n) {
  return n % (static_cast<uint64_t>(2) * k);
}

template<typename T, typename C, typename S, typename A>
uint64_t quantiles_sketch<T, C, S, A>::compute_bit_pattern(const uint16_t k, const uint64_t n) {
  return n / (static_cast<uint64_t>(2) * k);
}

template<typename T, typename C, typename S, typename A>
uint32_t quantiles_sketch<T, C, S, A>::compute_valid_levels(const uint64_t bit_pattern) {
  // TODO: Java's Long.bitCount() probably uses a better method
  uint64_t bp = bit_pattern;
  uint32_t count = 0;
  while (bp > 0) {
    if ((bp & 0x01) == 1) ++count;
    bp >>= 1;
  }
  return count;
}

template<typename T, typename C, typename S, typename A>
uint8_t quantiles_sketch<T, C, S, A>::compute_levels_needed(const uint16_t k, const uint64_t n) {
  return static_cast<uint8_t>(64U) - count_leading_zeros_in_u64(n / (2 * k));
}


template <typename T, typename C, typename S, typename A>
typename quantiles_sketch<T, C, S, A>::const_iterator quantiles_sketch<T, C, S, A>::begin() const {
  return quantiles_sketch<T, C, S, A>::const_iterator(base_buffer_, levels_, k_, n_, false);
}

template <typename T, typename C, typename S, typename A>
typename quantiles_sketch<T, C, S, A>::const_iterator quantiles_sketch<T, C, S, A>::end() const {
  return quantiles_sketch<T, C, S, A>::const_iterator(base_buffer_, levels_, k_, n_, true);
}

template<typename T, typename C, typename S, typename A>
void quantiles_sketch<T, C, S, A>::grow_base_buffer() {
  size_t new_size = std::max(std::min(static_cast<size_t>(2 * k_), 2 * base_buffer_.size()), static_cast<size_t>(1));
  base_buffer_.reserve(new_size);
}

template<typename T, typename C, typename S, typename A>
void quantiles_sketch<T, C, S, A>::process_full_base_buffer() {
  // make sure there will be enough levels for the propagation
  grow_levels_if_needed(); // note: n_ was already incremented by update() before this

  std::sort(base_buffer_.begin(), base_buffer_.end(), C());
  in_place_propagate_carry(0,
                           levels_[0], // unused here, but 0 is guaranteed to exist
                           base_buffer_,
                           true, *this);
  base_buffer_.clear();
  is_sorted_ = true;
  assert(n_ / (2 * k_) == bit_pattern_);  // internal consistency check
}

template<typename T, typename C, typename S, typename A>
bool quantiles_sketch<T, C, S, A>::grow_levels_if_needed() {
  uint8_t levels_needed = compute_levels_needed(k_, n_);
  if (levels_needed == 0)
    return false; // don't need levels and might have small base buffer. Possible during merges.

  // from here on, assume full size base buffer (2k) and at least one additional level
  if (levels_needed <= levels_.size())
    return false;

  Level empty_level(allocator_);
  empty_level.reserve(k_);
  levels_.push_back(std::move(empty_level));
  return true;
}

template<typename T, typename C, typename S, typename A>
void quantiles_sketch<T, C, S, A>::in_place_propagate_carry(uint8_t starting_level,
                                                            Level& buf_size_k, Level& buf_size_2k, 
                                                            bool apply_as_update,
                                                            quantiles_sketch<T,C,S,A>& sketch) {
  const uint64_t bit_pattern = sketch.bit_pattern_;
  const int k = sketch.k_;

  uint8_t ending_level = lowest_zero_bit_starting_at(bit_pattern, starting_level);

  if (apply_as_update) {
    // update version of computation
    // its is okay for buf_size_k to be null in this case
    zip_buffer(buf_size_2k, sketch.levels_[ending_level]);
  } else {
    // merge_into version of computation
    std::move(&buf_size_k[0], &buf_size_k[0] + k, &sketch.levels_[ending_level][0]);
  }

  for (uint64_t lvl = starting_level; lvl < ending_level; lvl++) {
    assert((bit_pattern & (static_cast<uint64_t>(1) << lvl)) > 0); // internal consistency check
    merge_two_size_k_buffers(
        sketch.levels_[lvl],
        sketch.levels_[ending_level],
        buf_size_2k);
    sketch.levels_[lvl].clear();
    sketch.levels_[ending_level].clear();
    zip_buffer(buf_size_2k, sketch.levels_[ending_level]);
  } // end of loop over lower levels

  // update bit pattern with binary-arithmetic ripple carry
  sketch.bit_pattern_ = bit_pattern + (static_cast<uint64_t>(1) << starting_level);
}

template<typename T, typename C, typename S, typename A>
void quantiles_sketch<T, C, S, A>::zip_buffer(Level& buf_in, Level& buf_out) {
#ifdef QUANTILES_VALIDATION
  static uint32_t next_offset = 0;
  uint32_t rand_offset = next_offset;
  next_offset = 1 - next_offset;
#else
  uint32_t rand_offset = random_bit();
#endif
  assert(buf_in.size() == 2 * buf_out.capacity());
  assert(buf_out.size() == 0);
  size_t k = buf_out.capacity();
  for (uint32_t i = rand_offset, o = 0; o < k; i += 2, ++o) {
    buf_out.push_back(std::move(buf_in[i]));
  }
  buf_in.clear();
}

template<typename T, typename C, typename S, typename A>
void quantiles_sketch<T, C, S, A>::merge_two_size_k_buffers(Level& src_1, Level& src_2, Level& dst) {
  assert(src_1.size() == src_2.size());
  assert(src_1.size() * 2 == dst.capacity());
  assert(dst.size() == 0);

  auto end1 = src_1.end(), end2 = src_2.end();
  auto it1 = src_1.begin(), it2 = src_2.begin();
  
  // TODO: probably actually doing copies given Level&?
  while (it1 != end1 && it2 != end2) {
    if (C()(*it1, *it2)) {
      dst.push_back(std::move(*it1++));
    } else {
      dst.push_back(std::move(*it2++));
    }
  }

  if (it1 != end1) {
    dst.insert(dst.end(), it1, end1);
  } else {
    assert(it2 != end2);
    dst.insert(dst.end(), it2, end2);
  }
}

template<typename T, typename C, typename S, typename A>
int quantiles_sketch<T, C, S, A>::lowest_zero_bit_starting_at(uint64_t bits, uint8_t starting_bit) {
  uint8_t pos = starting_bit & 0X3F;
  uint64_t my_bits = bits >> pos;

  while ((my_bits & static_cast<uint64_t>(1)) != 0) {
    my_bits >>= 1;
    pos++;
  }
  return pos;
}



// quantiles_sketch::const_iterator implementation

template<typename T, typename C, typename S, typename A>
quantiles_sketch<T, C, S, A>::const_iterator::const_iterator(const Level& base_buffer,
                                                             const std::vector<Level, AllocLevel>& levels,
                                                             uint16_t k,
                                                             uint64_t n,
                                                             bool is_end):
base_buffer_(base_buffer),
levels_(levels),
level_(-1),
index_(0),
k_(k),
bb_count_(compute_base_buffer_items(k, n)),
bit_pattern_(compute_bit_pattern(k, n)),
weight_(1)
{
  if (is_end) {
    // if exact mode: index_ = n is end
    // if sampling, level_ = max_level + 1 and index_ = 0 is end
    if (bit_pattern_ == 0) // only a valid check for exact mode in constructor
      index_ = static_cast<uint32_t>(n);
    else
      level_ = levels_.size();
  } else { // find first non-empty item
    if (bb_count_ == 0 && bit_pattern_ > 0) {
      level_ = 0;
      weight_ = 2;
      while ((bit_pattern_ & 0x01) == 0) {
        weight_ *= 2;
        ++level_;
        bit_pattern_ >>= 1;
      }
    }
  }
}

template<typename T, typename C, typename S, typename A>
typename quantiles_sketch<T, C, S, A>::const_iterator& quantiles_sketch<T, C, S, A>::const_iterator::operator++() {
  ++index_;

  if ((level_ == -1 && index_ == base_buffer_.size() && levels_.size() > 0) || (level_ >= 0 && index_ == k_)) { // go to the next non-empty level
    index_ = 0;
    do {
      ++level_;
      if (level_ > 0) bit_pattern_ = bit_pattern_ >> 1;
      if (bit_pattern_ == 0) return *this;
      weight_ *= 2;
    } while ((bit_pattern_ & static_cast<uint64_t>(1)) == 0);
  }
  return *this;
}

template<typename T, typename C, typename S, typename A>
typename quantiles_sketch<T, C, S, A>::const_iterator& quantiles_sketch<T, C, S, A>::const_iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename C, typename S, typename A>
bool quantiles_sketch<T, C, S, A>::const_iterator::operator==(const const_iterator& other) const {
  return level_ == other.level_ && index_ == other.index_;
}

template<typename T, typename C, typename S, typename A>
bool quantiles_sketch<T, C, S, A>::const_iterator::operator!=(const const_iterator& other) const {
  return !operator==(other);
}

template<typename T, typename C, typename S, typename A>
std::pair<const T&, const uint64_t> quantiles_sketch<T, C, S, A>::const_iterator::operator*() const {
  return std::pair<const T&, const uint64_t>(level_ == -1 ? base_buffer_[index_] : levels_[level_][index_], weight_);
}

} /* namespace datasketches */

#endif // _QUANTILES_SKETCH_IMPL_HPP_