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

namespace datasketches {

template<typename T, typename A>
tdigest<T, A>::tdigest(uint16_t k, const A& allocator):
allocator_(allocator),
k_(k),
internal_k_(k),
merge_count_(0),
min_(std::numeric_limits<T>::infinity()),
max_(-std::numeric_limits<T>::infinity()),
centroids_capacity_(0),
centroids_(allocator),
total_weight_(0),
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
  if (centroids_capacity_ < internal_k_ + fudge) {
    centroids_capacity_ = internal_k_ + fudge;
  }
  if (buffer_capacity_ < 2 * centroids_capacity_) buffer_capacity_ = 2 * centroids_capacity_;
}

template<typename T, typename A>
void tdigest<T, A>::update(T value) {
  // check for NaN
  if (buffer_.size() >= buffer_capacity_ - centroids_.size() - 1) merge_new_values(); // - 1 for compatibility with Java
  buffer_.push_back(centroid(value, 1));
  ++buffered_weight_;
  min_ = std::min(min_, value);
  max_ = std::max(max_, value);
}

template<typename T, typename A>
void tdigest<T, A>::merge(tdigest& other) {
}

template<typename T, typename A>
void tdigest<T, A>::compress() {
  merge_new_values(true, k_);
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
  return total_weight_ + buffered_weight_;
}

template<typename T, typename A>
double tdigest<T, A>::get_rank(T value) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  // check for NaN
  if (value < min_) return 0;
  if (value > max_) return 1;
  // one centroid and value == min_ == max_
  if ((centroids_.size() + buffer_.size()) == 1) return 0.5;

  const_cast<tdigest*>(this)->merge_new_values(); // side effect

  // left tail
  const T first_mean = centroids_.front().get_mean();
  if (value < first_mean) {
    if (first_mean - min_ > 0) {
      if (value == min_) return 0.5 / total_weight_;
      return (1.0 + (value - min_) / (first_mean - min_) * (centroids_.front().get_weight() / 2.0 - 1.0)); // ?
    }
    return 0; // should never happen
  }

  // right tail
  const T last_mean = centroids_.back().get_mean();
  if (value > last_mean) {
    if (max_ - last_mean > 0) {
      if (value == max_) return 1.0 - 0.5 / total_weight_;
        return 1 - ((1 + (max_ - value) / (max_ - last_mean) * (centroids_.back().get_weight() / 2.0 - 1.0)) / total_weight_); // ?
    }
    return 1; // should never happen
  }

  auto lower = std::lower_bound(centroids_.begin(), centroids_.end(), centroid(value, 1), centroid_cmp(false));
  if (lower == centroids_.end()) throw std::logic_error("lower == end in get_rank()");
  auto upper = std::upper_bound(lower, centroids_.end(), centroid(value, 1), centroid_cmp(false));
  if (upper == centroids_.begin()) throw std::logic_error("upper == begin in get_rank()");
  if (value < lower->get_mean()) --lower;
  if (upper == centroids_.end() || (upper != centroids_.begin() && !((upper - 1)->get_mean() < value))) --upper;
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
    return (weight_below + weight_delta * (value - lower->get_mean()) / (upper->get_mean() - lower->get_mean())) / total_weight_;
  }
  return (weight_below + weight_delta / 2.0) / total_weight_;
}

template<typename T, typename A>
T tdigest<T, A>::get_quantile(double rank) const {
  if (is_empty()) throw std::runtime_error("operation is undefined for an empty sketch");
  return 0;
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
  os << "   Total Weight       : " << total_weight_ << std::endl;
  os << "   Buffered Weight    : " << buffered_weight_ << std::endl;
  os << "   Merge count        : " << merge_count_ << std::endl;
  if (!is_empty()) {
    os << "   Min                : " << min_ << std::endl;
    os << "   Max                : " << max_ << std::endl;
  }
  os << "### End t-Digest summary" << std::endl;
  if (print_centroids) {
    os << "Centroids:" << std::endl;
    int i = 0;
    for (auto centroid: centroids_) {
      os << i << ": " << centroid.get_mean() << ", " << centroid.get_weight() << std::endl;
      ++i;
    }
  }
  return string<A>(os.str().c_str(), allocator_);
}

template<typename T, typename A>
void tdigest<T, A>::merge_new_values() {
  merge_new_values(false, internal_k_);
}

template<typename T, typename A>
void tdigest<T, A>::merge_new_values(bool force, uint16_t k) {
  if (total_weight_ == 0 && buffered_weight_ == 0) return;
  if (force || buffered_weight_ > 0) {
    merge_new_values(buffer_, buffered_weight_, k, USE_ALTERNATING_SORT & (merge_count_ & 1));
    ++merge_count_;
    buffer_.clear();
    buffered_weight_ = 0;
  }
}

template<typename T, typename A>
void tdigest<T, A>::merge_new_values(vector_centroid& incoming_centroids, uint64_t weight, uint16_t k, bool reverse) {
  for (const auto& centroid: centroids_) incoming_centroids.push_back(centroid);
  centroids_.clear();
  std::stable_sort(incoming_centroids.begin(), incoming_centroids.end(), centroid_cmp(reverse));
  total_weight_ += weight;
  auto it = incoming_centroids.begin();
  centroids_.push_back(*it);
  ++it;
  double weight_so_far = 0;
  const double normalizer = scale_function().normalizer(k, total_weight_);
  double k1 = scale_function().k(0, normalizer);
  double w_limit = total_weight_ * scale_function().q(k1 + 1, normalizer);
  while (it != incoming_centroids.end()) {
    const double proposed_weight = centroids_.back().get_weight() + it->get_weight();
    const double projected_weight = weight_so_far + proposed_weight;
    bool add_this;
    if (USE_WEIGHT_LIMIT) {
      const double q0 = weight_so_far / total_weight_;
      const double q2 = (weight_so_far + proposed_weight) / total_weight_;
      add_this = proposed_weight <= total_weight_ * std::min(scale_function().max(q0, normalizer), scale_function().max(q2, normalizer));
    } else {
      add_this = projected_weight <= w_limit;
    }
    if (std::distance(incoming_centroids.begin(), it) == 1 || std::distance(incoming_centroids.end(), it) == 1) {
      add_this = false;
    }
    if (add_this) {
      centroids_.back().add(*it);
    } else {
      weight_so_far += centroids_.back().get_weight();
      if (!USE_WEIGHT_LIMIT) {
        k1 = scale_function().k(weight_so_far / total_weight_, normalizer);
        w_limit = total_weight_ * scale_function().q(k1 + 1, normalizer);
      }
      centroids_.push_back(*it);
    }
    ++it;
  }
  if (reverse) std::reverse(centroids_.begin(), centroids_.end());
  if (total_weight_ > 0) {
    min_ = std::min(min_, centroids_.front().get_mean());
    max_ = std::max(max_, centroids_.back().get_mean());
  }
}

} /* namespace datasketches */

#endif // _TDIGEST_IMPL_HPP_
