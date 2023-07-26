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

#ifndef _EBPPS_SKETCH_IMPL_HPP_
#define _EBPPS_SKETCH_IMPL_HPP_

#include <memory>
#include <sstream>
#include <cmath>
#include <random>
#include <algorithm>
#include <stdexcept>
#include <utility>

#include "ebpps_sketch.hpp"
//#include "serde.hpp"

namespace datasketches {

/*
 * Implementation code for the Exact PPS Sampling with Bounded Sample Size sketch.
 * 
 * author Jon Malkin
 */
template<typename T, typename A>
ebpps_sketch<T, A>::ebpps_sketch(uint32_t k, const A& allocator) :
  allocator_(allocator),
  k_(k),
  n_(0),
  cumulative_wt_(0.0),
  wt_max_(0.0),
  rho_(1.0),
  sample_(k)
  {
    if (k == 0 || k > MAX_K)
      throw std::invalid_argument("k must be strictly positive and less than " + std::to_string(MAX_K));
  }

template<typename T, typename A>
uint32_t ebpps_sketch<T, A>::get_k() const {
  return k_;
}

template<typename T, typename A>
uint64_t ebpps_sketch<T, A>::get_n() const {
  return n_;
}

template<typename T, typename A>
double ebpps_sketch<T, A>::get_c() const {
  return sample_.get_c();
}

template<typename T, typename A>
double ebpps_sketch<T, A>::get_cumulative_weight() const {
  return cumulative_wt_;
}

template<typename T, typename A>
bool ebpps_sketch<T, A>::is_empty() const {
  return n_ == 0;
}

template<typename T, typename A>
void ebpps_sketch<T, A>::reset() {
  n_ = 0;
  cumulative_wt_ = 0.0;
  wt_max_ = 0.0;
  rho_ = 1.0;
  sample_.reset();
}

template<typename T, typename A>
void ebpps_sketch<T, A>::update(const T& item, double weight) {
  return internal_update(item, weight);
}

template<typename T, typename A>
void ebpps_sketch<T, A>::update(T&& item, double weight) {
  return internal_update(std::move(item), weight);
}

template<typename T, typename A>
template<typename FwdItem>
void ebpps_sketch<T, A>::internal_update(FwdItem&& item, double weight) {
  double new_cum_wt = cumulative_wt_ + weight;
  double new_wt_max = std::max(wt_max_, weight);
  double new_rho = std::min(1.0 / new_wt_max, k_ / new_cum_wt);

  if (cumulative_wt_ > 0.0)
    sample_.downsample(new_rho / rho_);
  
  ebpps_sample<T,A> tmp(conditional_forward<FwdItem>(item), new_rho * weight, allocator_);

  sample_.merge(tmp);

  cumulative_wt_ = new_cum_wt;
  wt_max_ = new_wt_max;
  rho_ = new_rho;
  ++n_;
}

template<typename T, typename A>
auto ebpps_sketch<T,A>::get_result() const -> result_type {
  return sample_.get_sample();
}

template<typename T, typename A>
void ebpps_sketch<T, A>::merge(const ebpps_sketch<T>& sk) {
  double new_cum_wt = cumulative_wt_ + sk.cumulative_wt_;
  double new_wt_max = std::max(wt_max_, sk.wt_max_);
  double new_rho = std::min(1.0 / new_wt_max, std::min(k_, sk.k_) / new_cum_wt);

  sample_.downsample(new_rho / rho_);

  ebpps_sample<T,A> other_sample(sk.sample_);
  other_sample.downsample(new_rho / sk.rho_);

  sample_.merge(other_sample);

  cumulative_wt_ = new_cum_wt;
  wt_max_ = new_wt_max;
  rho_ = new_rho;
  n_ += sk.n_;
}

template<typename T, typename A>
void ebpps_sketch<T, A>::merge(ebpps_sketch<T>&& sk) {
  double new_cum_wt = cumulative_wt_ + sk.cumulative_wt_;
  double new_wt_max = std::max(wt_max_, sk.wt_max_);
  double new_rho = std::min(1.0 / new_wt_max, std::min(k_, sk.k_) / new_cum_wt);

  sample_.downsample(new_rho / rho_);
  sk.sample_.downsample(new_rho / sk.rho_);
  sample_.merge(sk.sample_);

  cumulative_wt_ = new_cum_wt;
  wt_max_ = new_wt_max;
  rho_ = new_rho;
  n_ += sk.n_;
}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator ebpps_sketch<T, A>::begin() const {
  return sample_.begin();
}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator ebpps_sketch<T, A>::end() const {
  return sample_.end();
}

/*
// -------- ebpps_sketch::const_iterator implementation ---------

template<typename T, typename A>
ebpps_sketch<T, A>::const_iterator::const_iterator(const ebpps_sketch& sk, bool is_end, bool force_partial) :
  sk_(&sk),
  idx_(is_end ? sk.data_.end() : 0),
  use_partial_(force_partial)
{
  // determine in advance if we use the partial item
  if (!force_partial) {
    double c_int;
    double c_frac = std::modf(sk.c_, &c_int);
    use_partial_ = random_utils::next_double() < c_frac;
  }

  if (sk.data_.size() == 0 && use_partial_) {
    idx = PARTIAL_IDX;
  }

  if (sk.is_empty()) { sk_ = nullptr; }
}

template<typename T, typename A>
ebpps_sketch<T, A>::const_iterator::const_iterator(const const_iterator& other) :
  sk_(other.sk_),
  idx_(other.idx),
  use_partial_(other.use_partial_)
{}

template<typename T, typename A>
typename ebpps_sketch<T, A>::const_iterator& ebpps_sketch<T, A>::const_iterator::operator++() {
  if (sk_ == nullptr)
    return *this;

  ++idx_;

  if (idx_ == data_.end()) {
    if (use_partial_)
      idx_ = PARTIAL_IDX;
    else
      sk_ = nullptr;
  } else if (idx_ == PARTIAL_IDX) {
    sk_ = nullptr;
    idx_ = data_.end();
  }

  return *this;
}

template<typename T, typename A>
typename ebpps_sketch<T, A>::const_iterator& ebpps_sketch<T, A>::const_iterator::operator++(int) {
  const_iterator tmp(*this);
  operator++();
  return tmp;
}

template<typename T, typename A>
bool ebpps_sketch<T, A>::const_iterator::operator==(const const_iterator& other) const {
  if (sk_ != other.sk_) return false;
  if (sk_ == nullptr) return true; // end (and we know other.sk_ is also null)
  return idx_ == other.idx_;
}

template<typename T, typename A>
bool ebpps_sketch<T, A>::const_iterator::operator!=(const const_iterator& other) const {
  return !operator==(other);
}

template<typename T, typename A>
auto ebpps_sketch<T, A>::const_iterator::operator*() const -> reference {
  if (idx_ == PARTIAL_IDX)
    return value_type(sk_.get)

  double wt;
  if (idx_ < sk_->h_) {
    wt = sk_->weights_[idx_];
  } else {
    wt = r_item_wt_;
  }
  return value_type(sk_->data_[idx_], wt);
}

template<typename T, typename A>
auto ebpps_sketch<T, A>::const_iterator::operator->() const -> pointer {
  return **this;
}
*/
} // namespace datasketches

#endif // _EBPPS_SKETCH_IMPL_HPP_
