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

#ifndef _TDIGEST_IMPL_HPP_
#define _TDIGEST_IMPL_HPP_

#include <cmath>
#include <sstream>

#include "common_defs.hpp"
#include "memory_operations.hpp"

namespace datasketches {

template<typename T, typename A>
tdigest<T, A>::tdigest(uint16_t k, const A& allocator):
tdigest(false, k, std::numeric_limits<T>::infinity(), -std::numeric_limits<T>::infinity(), vector_centroid(allocator), 0, allocator)
{}

template<typename T, typename A>
void tdigest<T, A>::update(T value) {
  if (std::isnan(value)) return;
  if (buffer_.size() >= buffer_capacity_ - centroids_.size()) merge_buffered();
  buffer_.push_back(centroid(value, 1));
  ++buffered_weight_;
  min_ = std::min(min_, value);
  max_ = std::max(max_, value);
}

template<typename T, typename A>
void tdigest<T, A>::merge(tdigest& other) {
  if (other.is_empty()) return;
  size_t num = buffer_.size() + centroids_.size() + other.buffer_.size() + other.centroids_.size();
  buffer_.reserve(num);
  std::copy(other.buffer_.begin(), other.buffer_.end(), std::back_inserter(buffer_));
  std::copy(other.centroids_.begin(), other.centroids_.end(), std::back_inserter(buffer_));
  buffered_weight_ += other.get_total_weight();
  if (num > buffer_capacity_) {
    merge_buffered();
  } else {
    min_ = std::min(min_, other.get_min_value());
    max_ = std::max(max_, other.get_max_value());
  }
}

template<typename T, typename A>
void tdigest<T, A>::compress() {
  merge_buffered();
}

template<typename T, typename A>
bool tdigest<T, A>::is_empty() const {
  return centroids_.empty() && buffer_.empty();
}

template<typename T, typename A>
T tdigest<T, A>::get_min_value() const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  return min_;
}

template<typename T, typename A>
T tdigest<T, A>::get_max_value() const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  return max_;
}

template<typename T, typename A>
uint64_t tdigest<T, A>::get_total_weight() const {
  return centroids_weight_ + buffered_weight_;
}

template<typename T, typename A>
double tdigest<T, A>::get_rank(T value) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  if (std::isnan(value)) throw std::invalid_argument("operation is undefined for NaN");
  if (value < min_) return 0;
  if (value > max_) return 1;
  // one centroid and value == min_ == max_
  if ((centroids_.size() + buffer_.size()) == 1) return 0.5;

  const_cast<tdigest*>(this)->merge_buffered(); // side effect

  // left tail
  const T first_mean = centroids_.front().get_mean();
  if (value < first_mean) {
    if (first_mean - min_ > 0) {
      if (value == min_) return 0.5 / centroids_weight_;
      return (1.0 + (value - min_) / (first_mean - min_) * (centroids_.front().get_weight() / 2.0 - 1.0)); // ?
    }
    return 0; // should never happen
  }

  // right tail
  const T last_mean = centroids_.back().get_mean();
  if (value > last_mean) {
    if (max_ - last_mean > 0) {
      if (value == max_) return 1.0 - 0.5 / centroids_weight_;
        return 1.0 - ((1.0 + (max_ - value) / (max_ - last_mean) * (centroids_.back().get_weight() / 2.0 - 1.0)) / centroids_weight_); // ?
    }
    return 1; // should never happen
  }

  auto lower = std::lower_bound(centroids_.begin(), centroids_.end(), centroid(value, 1), centroid_cmp());
  if (lower == centroids_.end()) throw std::logic_error("lower == end in get_rank()");
  auto upper = std::upper_bound(lower, centroids_.end(), centroid(value, 1), centroid_cmp());
  if (upper == centroids_.begin()) throw std::logic_error("upper == begin in get_rank()");
  if (value < lower->get_mean()) --lower;
  if (upper == centroids_.end() || !((upper - 1)->get_mean() < value)) --upper;
  double weight_below = 0;
  auto it = centroids_.begin();
  while (it != lower) {
    weight_below += it->get_weight();
    ++it;
  }
  weight_below += lower->get_weight() / 2.0;
  double weight_delta = 0;
  while (it != upper) {
    weight_delta += it->get_weight();
    ++it;
  }
  weight_delta -= lower->get_weight() / 2.0;
  weight_delta += upper->get_weight() / 2.0;
  if (upper->get_mean() - lower->get_mean() > 0) {
    return (weight_below + weight_delta * (value - lower->get_mean()) / (upper->get_mean() - lower->get_mean())) / centroids_weight_;
  }
  return (weight_below + weight_delta / 2.0) / centroids_weight_;
}

template<typename T, typename A>
T tdigest<T, A>::get_quantile(double rank) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  if ((rank < 0.0) || (rank > 1.0)) {
    throw std::invalid_argument("Normalized rank cannot be less than 0 or greater than 1");
  }
  const_cast<tdigest*>(this)->merge_buffered(); // side effect
  if (centroids_.size() == 1) return centroids_.front().get_mean();

  // at least 2 centroids
  const double weight = rank * centroids_weight_;
  if (weight < 1) return min_;
  if (weight > centroids_weight_ - 1.0) return max_;
  const double first_weight = centroids_.front().get_weight();
  if (first_weight > 1 && weight < first_weight / 2.0) {
    return min_ + (weight - 1.0) / (first_weight / 2.0 - 1.0) * (centroids_.front().get_mean() - min_);
  }
  const double last_weight = centroids_.back().get_weight();
  if (last_weight > 1 && centroids_weight_ - weight <= last_weight / 2.0) {
    return max_ + (centroids_weight_ - weight - 1.0) / (last_weight / 2.0 - 1.0) * (max_ - centroids_.back().get_mean());
  }

  // interpolate between extremes
  double weight_so_far = first_weight / 2.0;
  for (size_t i = 0; i < centroids_.size() - 1; ++i) {
    const double dw = (centroids_[i].get_weight() + centroids_[i + 1].get_weight()) / 2.0;
    if (weight_so_far + dw > weight) {
      // the target weight is between centroids i and i+1
      double left_weight = 0;
      if (centroids_[i].get_weight() == 1) {
        if (weight - weight_so_far < 0.5) return centroids_[i].get_mean();
        left_weight = 0.5;
      }
      double right_weight = 0;
      if (centroids_[i + 1].get_weight() == 1) {
        if (weight_so_far + dw - weight <= 0.5) return centroids_[i + 1].get_mean();
        right_weight = 0.5;
      }
      const double w1 = weight - weight_so_far - left_weight;
      const double w2 = weight_so_far + dw - weight - right_weight;
      return weighted_average(centroids_[i].get_mean(), w1, centroids_[i + 1].get_mean(), w2);
    }
    weight_so_far += dw;
  }
  const double w1 = weight - centroids_weight_ - centroids_.back().get_weight() / 2.0;
  const double w2 = centroids_.back().get_weight() / 2.0 - w1;
  return weighted_average(centroids_.back().get_weight(), w1, max_, w2);
}

template<typename T, typename A>
uint16_t tdigest<T, A>::get_k() const {
  return k_;
}

template<typename T, typename A>
string<A> tdigest<T, A>::to_string(bool print_centroids) const {
  // Using a temporary stream for implementation here does not comply with AllocatorAwareContainer requirements.
  // The stream does not support passing an allocator instance, and alternatives are complicated.
  std::ostringstream os;
  os << "### t-Digest summary:" << std::endl;
  os << "   Nominal k          : " << k_ << std::endl;
  os << "   Internal k         : " << internal_k_ << std::endl;
  os << "   Centroids          : " << centroids_.size() << std::endl;
  os << "   Buffered           : " << buffer_.size() << std::endl;
  os << "   Centroids capacity : " << centroids_capacity_ << std::endl;
  os << "   Buffer capacity    : " << buffer_capacity_ << std::endl;
  os << "   Centroids Weight   : " << centroids_weight_ << std::endl;
  os << "   Buffered Weight    : " << buffered_weight_ << std::endl;
  os << "   Total Weight       : " << get_total_weight() << std::endl;
  os << "   Reverse Merge      : " << (reverse_merge_ ? "true" : "false") << std::endl;
  if (!is_empty()) {
    os << "   Min                : " << min_ << std::endl;
    os << "   Max                : " << max_ << std::endl;
  }
  os << "### End t-Digest summary" << std::endl;
  if (print_centroids) {
    if (centroids_.size() > 0) {
      os << "Centroids:" << std::endl;
      int i = 0;
      for (const auto& c: centroids_) {
        os << i++ << ": " << c.get_mean() << ", " << c.get_weight() << std::endl;
      }
    }
    if (buffer_.size() > 0) {
      os << "Buffer:" << std::endl;
      int i = 0;
      for (const auto& b: buffer_) {
        os << i++ << ": " << b.get_mean() << ", " << b.get_weight() << std::endl;
      }
    }
  }
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename A>
void tdigest<T, A>::merge_buffered() {
  if (buffered_weight_ == 0) return;
  const bool reverse = USE_ALTERNATING_SORT && reverse_merge_;
  std::copy(centroids_.begin(), centroids_.end(), std::back_inserter(buffer_));
  centroids_.clear();
  std::stable_sort(buffer_.begin(), buffer_.end(), centroid_cmp());
  if (reverse) std::reverse(buffer_.begin(), buffer_.end());
  centroids_weight_ += buffered_weight_;
  auto it = buffer_.begin();
  centroids_.push_back(*it);
  ++it;
  double weight_so_far = 0;
  const double normalizer = scale_function().normalizer(internal_k_, centroids_weight_);
  double k1 = scale_function().k(0, normalizer);
  double w_limit = centroids_weight_ * scale_function().q(k1 + 1, normalizer);
  while (it != buffer_.end()) {
    const double proposed_weight = centroids_.back().get_weight() + it->get_weight();
    bool add_this;
    if (std::distance(buffer_.begin(), it) == 1 || std::distance(buffer_.end(), it) == 1) {
      add_this = false;
    } else if (USE_WEIGHT_LIMIT) {
      const double q0 = weight_so_far / centroids_weight_;
      const double q2 = (weight_so_far + proposed_weight) / centroids_weight_;
      add_this = proposed_weight <= centroids_weight_ * std::min(scale_function().max(q0, normalizer), scale_function().max(q2, normalizer));
    } else {
      add_this = weight_so_far + proposed_weight <= w_limit;
    }
    if (add_this) {
      centroids_.back().add(*it);
    } else {
      weight_so_far += centroids_.back().get_weight();
      if (!USE_WEIGHT_LIMIT) {
        k1 = scale_function().k(weight_so_far / centroids_weight_, normalizer);
        w_limit = centroids_weight_ * scale_function().q(k1 + 1, normalizer);
      }
      centroids_.push_back(*it);
    }
    ++it;
  }
  if (reverse) std::reverse(centroids_.begin(), centroids_.end());
  if (centroids_weight_ > 0) {
    min_ = std::min(min_, centroids_.front().get_mean());
    max_ = std::max(max_, centroids_.back().get_mean());
  }
  reverse_merge_ = !reverse_merge_;
  buffer_.clear();
  buffered_weight_ = 0;
}

template<typename T, typename A>
double tdigest<T, A>::weighted_average(double x1, double w1, double x2, double w2) {
  return (x1 * w1 + x2 * w2) / (w1 + w2);
}

template<typename T, typename A>
void tdigest<T, A>::serialize(std::ostream& os) const {
  const_cast<tdigest*>(this)->merge_buffered(); // side effect
  write(os, is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_NON_EMPTY);
  write(os, SERIAL_VERSION);
  write(os, SKETCH_TYPE);
  write(os, k_);
  const uint8_t flags_byte(
    (is_empty() ? 1 << flags::IS_EMPTY : 0) |
    (reverse_merge_ ? 1 << flags::REVERSE_MERGE : 0)
  );
  write(os, flags_byte);
  write<uint16_t>(os, 0); // unused

  if (is_empty()) return;

  write(os, static_cast<uint32_t>(centroids_.size()));
  write<uint32_t>(os, 0); // unused

  write(os, min_);
  write(os, max_);
  write(os, centroids_.data(), centroids_.size() * sizeof(centroid));
}

template<typename T, typename A>
auto tdigest<T, A>::serialize(unsigned header_size_bytes) const -> vector_bytes {
  const_cast<tdigest*>(this)->merge_buffered(); // side effect
  const uint8_t preamble_longs = is_empty() ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_NON_EMPTY;
  const size_t size_bytes = preamble_longs * sizeof(uint64_t) + (is_empty() ? 0 : sizeof(T) * 2 + sizeof(centroid) * centroids_.size());
  vector_bytes bytes(size_bytes, 0, allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;

  *ptr++ = preamble_longs;
  *ptr++ = SERIAL_VERSION;
  *ptr++ = SKETCH_TYPE;
  ptr += copy_to_mem(k_, ptr);
  const uint8_t flags_byte(
    (is_empty() ? 1 << flags::IS_EMPTY : 0) |
    (reverse_merge_ ? 1 << flags::REVERSE_MERGE : 0)
  );
  *ptr++ = flags_byte;
  ptr += 2; // unused
  if (is_empty()) return bytes;

  ptr += copy_to_mem(static_cast<uint32_t>(centroids_.size()), ptr);
  ptr += 4; // unused

  ptr += copy_to_mem(min_, ptr);
  ptr += copy_to_mem(max_, ptr);
  copy_to_mem(centroids_.data(), ptr, centroids_.size() * sizeof(centroid));
  return bytes;
}

template<typename T, typename A>
tdigest<T, A> tdigest<T, A>::deserialize(std::istream& is, const A& allocator) {
  const auto preamble_longs = read<uint8_t>(is);
  const auto serial_version = read<uint8_t>(is);
  const auto sketch_type = read<uint8_t>(is);
  if (sketch_type != SKETCH_TYPE) {
    if (preamble_longs == 0 && serial_version == 0 && sketch_type == 0) return deserialize_compat(is, allocator);
    throw std::invalid_argument("sketch type mismatch: expected " + std::to_string(SKETCH_TYPE) + ", actual " + std::to_string(sketch_type));
  }
  if (serial_version != SERIAL_VERSION) {
    throw std::invalid_argument("serial version mismatch: expected " + std::to_string(SERIAL_VERSION) + ", actual " + std::to_string(serial_version));
  }
  const auto k = read<uint16_t>(is);
  const auto flags_byte = read<uint8_t>(is);
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  const uint8_t expected_preamble_longs = is_empty ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_NON_EMPTY;
  if (preamble_longs != expected_preamble_longs) {
    throw std::invalid_argument("preamble longs mismatch: expected " + std::to_string(expected_preamble_longs) + ", actual " + std::to_string(preamble_longs));
  }
  read<uint16_t>(is); // unused

  if (is_empty) return tdigest(k, allocator);

  const auto num_centroids = read<uint32_t>(is);
  read<uint32_t>(is); // unused

  const T min = read<T>(is);
  const T max = read<T>(is);
  vector_centroid centroids(num_centroids, centroid(0, 0), allocator);
  read(is, centroids.data(), num_centroids * sizeof(centroid));
  uint64_t total_weight = 0;
  for (const auto& c: centroids) total_weight += c.get_weight();
  const bool reverse_merge = flags_byte & (1 << flags::REVERSE_MERGE);
  return tdigest(reverse_merge, k, min, max, std::move(centroids), total_weight, allocator);
}

template<typename T, typename A>
tdigest<T, A> tdigest<T, A>::deserialize(const void* bytes, size_t size, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  const char* end_ptr = static_cast<const char*>(bytes) + size;

  const uint8_t preamble_longs = *ptr++;
  const uint8_t serial_version = *ptr++;
  const uint8_t sketch_type = *ptr++;
  if (sketch_type != SKETCH_TYPE) {
    if (preamble_longs == 0 && serial_version == 0 && sketch_type == 0) return deserialize_compat(ptr, end_ptr - ptr, allocator);
    throw std::invalid_argument("sketch type mismatch: expected " + std::to_string(SKETCH_TYPE) + ", actual " + std::to_string(sketch_type));
  }
  if (serial_version != SERIAL_VERSION) {
    throw std::invalid_argument("serial version mismatch: expected " + std::to_string(SERIAL_VERSION) + ", actual " + std::to_string(serial_version));
  }
  uint16_t k;
  ptr += copy_from_mem(ptr, k);
  const uint8_t flags_byte = *ptr++;
  const bool is_empty = flags_byte & (1 << flags::IS_EMPTY);
  const uint8_t expected_preamble_longs = is_empty ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_NON_EMPTY;
  if (preamble_longs != expected_preamble_longs) {
    throw std::invalid_argument("preamble longs mismatch: expected " + std::to_string(expected_preamble_longs) + ", actual " + std::to_string(preamble_longs));
  }
  ptr += 2; // unused

  if (is_empty) return tdigest(k, allocator);

  ensure_minimum_memory(end_ptr - ptr, 8);
  uint32_t num_centroids;
  ptr += copy_from_mem(ptr, num_centroids);
  ptr += 4; // unused

  ensure_minimum_memory(end_ptr - ptr, sizeof(T) * 2 + sizeof(centroid) * num_centroids);
  T min;
  ptr += copy_from_mem(ptr, min);
  T max;
  ptr += copy_from_mem(ptr, max);
  vector_centroid centroids(num_centroids, centroid(0, 0), allocator);
  copy_from_mem(ptr, centroids.data(), sizeof(centroid) * num_centroids);
  uint64_t total_weight = 0;
  for (const auto& c: centroids) total_weight += c.get_weight();
  const bool reverse_merge = flags_byte & (1 << flags::REVERSE_MERGE);
  return tdigest(reverse_merge, k, min, max, std::move(centroids), total_weight, allocator);
}

// compatibility with the format of the reference implementation
// default byte order of ByteBuffer is used there, which is big endian
template<typename T, typename A>
tdigest<T, A> tdigest<T, A>::deserialize_compat(std::istream& is, const A& allocator) {
  // this method was called because the first three bytes were zeros
  // so read one more byte to see if it looks like the reference implementation format
  const auto type = read<uint8_t>(is);
  if (type != COMPAT_DOUBLE && type != COMPAT_FLOAT) {
    throw std::invalid_argument("unexpected sketch preamble: 0 0 0 " + std::to_string(type));
  }
  if (type == COMPAT_DOUBLE) { // compatibility with asBytes()
    const auto min = read_big_endian<double>(is);
    const auto max = read_big_endian<double>(is);
    const auto k = static_cast<uint16_t>(read_big_endian<double>(is));
    const auto num_centroids = read_big_endian<uint32_t>(is);
    vector_centroid centroids(num_centroids, centroid(0, 0), allocator);
    uint64_t total_weight = 0;
    for (auto& c: centroids) {
      const uint64_t weight = static_cast<uint64_t>(read_big_endian<double>(is));
      const auto mean = read_big_endian<double>(is);
      c = centroid(mean, weight);
      total_weight += weight;
    }
    return tdigest(false, k, min, max, std::move(centroids), total_weight, allocator);
  }
  // COMPAT_FLOAT: compatibility with asSmallBytes()
  const auto min = read_big_endian<double>(is); // reference implementation uses doubles for min and max
  const auto max = read_big_endian<double>(is);
  const auto k = static_cast<uint16_t>(read_big_endian<float>(is));
  // reference implementation stores capacities of the array of centroids and the buffer as shorts
  // they can be derived from k in the constructor
  read<uint32_t>(is); // unused
  const auto num_centroids = read_big_endian<uint16_t>(is);
  vector_centroid centroids(num_centroids, centroid(0, 0), allocator);
  uint64_t total_weight = 0;
  for (auto& c: centroids) {
    const uint64_t weight = static_cast<uint64_t>(read_big_endian<float>(is));
    const auto mean = read_big_endian<float>(is);
    c = centroid(mean, weight);
    total_weight += weight;
  }
  return tdigest(false, k, min, max, std::move(centroids), total_weight, allocator);
}

// compatibility with the format of the reference implementation
// default byte order of ByteBuffer is used there, which is big endian
template<typename T, typename A>
tdigest<T, A> tdigest<T, A>::deserialize_compat(const void* bytes, size_t size, const A& allocator) {
  const char* ptr = static_cast<const char*>(bytes);
  // this method was called because the first three bytes were zeros
  // so read one more byte to see if it looks like the reference implementation format
  const auto type = *ptr++;
  if (type != COMPAT_DOUBLE && type != COMPAT_FLOAT) {
    throw std::invalid_argument("unexpected sketch preamble: 0 0 0 " + std::to_string(type));
  }
  const char* end_ptr = static_cast<const char*>(bytes) + size;
  if (type == COMPAT_DOUBLE) { // compatibility with asBytes()
    ensure_minimum_memory(end_ptr - ptr, sizeof(double) * 3 + sizeof(uint32_t));
    double min;
    ptr += copy_from_mem(ptr, min);
    min = byteswap(min);
    double max;
    ptr += copy_from_mem(ptr, max);
    max = byteswap(max);
    double k_double;
    ptr += copy_from_mem(ptr, k_double);
    const uint16_t k = static_cast<uint16_t>(byteswap(k_double));
    uint32_t num_centroids;
    ptr += copy_from_mem(ptr, num_centroids);
    num_centroids = byteswap(num_centroids);
    ensure_minimum_memory(end_ptr - ptr, sizeof(double) * num_centroids * 2);
    vector_centroid centroids(num_centroids, centroid(0, 0), allocator);
    uint64_t total_weight = 0;
    for (auto& c: centroids) {
      double weight;
      ptr += copy_from_mem(ptr, weight);
      weight = byteswap(weight);
      double mean;
      ptr += copy_from_mem(ptr, mean);
      mean = byteswap(mean);
      c = centroid(mean, static_cast<uint64_t>(weight));
      total_weight += static_cast<uint64_t>(weight);
    }
    return tdigest(false, k, min, max, std::move(centroids), total_weight, allocator);
  }
  // COMPAT_FLOAT: compatibility with asSmallBytes()
  ensure_minimum_memory(end_ptr - ptr, sizeof(double) * 2 + sizeof(float) + sizeof(uint16_t) * 3);
  double min; // reference implementation uses doubles for min and max
  ptr += copy_from_mem(ptr, min);
  min = byteswap(min);
  double max;
  ptr += copy_from_mem(ptr, max);
  max = byteswap(max);
  float k_float;
  ptr += copy_from_mem(ptr, k_float);
  const uint16_t k = static_cast<uint16_t>(byteswap(k_float));
  // reference implementation stores capacities of the array of centroids and the buffer as shorts
  // they can be derived from k in the constructor
  ptr += sizeof(uint32_t); // unused
  uint16_t num_centroids;
  ptr += copy_from_mem(ptr, num_centroids);
  num_centroids = byteswap(num_centroids);
  ensure_minimum_memory(end_ptr - ptr, sizeof(float) * num_centroids * 2);
  vector_centroid centroids(num_centroids, centroid(0, 0), allocator);
  uint64_t total_weight = 0;
  for (auto& c: centroids) {
    float weight;
    ptr += copy_from_mem(ptr, weight);
    weight = byteswap(weight);
    float mean;
    ptr += copy_from_mem(ptr, mean);
    mean = byteswap(mean);
    c = centroid(mean, static_cast<uint64_t>(weight));
    total_weight += static_cast<uint64_t>(weight);
  }
  return tdigest(false, k, min, max, std::move(centroids), total_weight, allocator);
}

template<typename T, typename A>
tdigest<T, A>::tdigest(bool reverse_merge, uint16_t k, T min, T max, vector_centroid&& centroids, uint64_t total_weight, const A& allocator):
allocator_(allocator),
reverse_merge_(reverse_merge),
k_(k),
internal_k_(k),
min_(min),
max_(max),
centroids_capacity_(0),
centroids_(std::move(centroids)),
centroids_weight_(total_weight),
buffer_capacity_(0),
buffer_(allocator),
buffered_weight_(0)
{
  if (k < 10) throw std::invalid_argument("k must be at least 10");
  size_t fudge = 0;
  if (USE_WEIGHT_LIMIT) {
    fudge = 10;
    if (k < 30) fudge +=20;
  }
  centroids_capacity_ = 2 * k_ + fudge;
  buffer_capacity_ = 5 * centroids_capacity_;
  double scale = std::max(1.0, static_cast<double>(buffer_capacity_) / centroids_capacity_ - 1.0);
  if (!USE_TWO_LEVEL_COMPRESSION) scale = 1;
  internal_k_ = std::ceil(std::sqrt(scale) * k_);
  centroids_capacity_ = std::max(centroids_capacity_, internal_k_ + fudge);
  buffer_capacity_ = std::max(buffer_capacity_, 2 * centroids_capacity_);
  centroids_.reserve(centroids_capacity_);
  buffer_.reserve(buffer_capacity_);
}

} /* namespace datasketches */

#endif // _TDIGEST_IMPL_HPP_
