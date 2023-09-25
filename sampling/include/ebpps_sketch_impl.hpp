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
ebpps_sketch<T,A>::ebpps_sketch(uint32_t k, uint64_t n, double cumulative_wt,
                                double wt_max, double rho,
                                ebpps_sample<T,A>&& sample, const A& allocator) :
  allocator_(allocator),
  k_(k),
  n_(n),
  cumulative_wt_(cumulative_wt),
  wt_max_(wt_max),
  rho_(rho),
  sample_(sample)
  {}

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
  if (weight < 0.0 || std::isnan(weight) || std::isinf(weight)) {
    throw std::invalid_argument("Item weights must be nonnegative and finite. Found: "
                                + std::to_string(weight));
  } else if (weight == 0.0) {
    return;
  }

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

/*
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
*/

/* Merging
 * There is a trivial merge algorithm that involves downsampling each sketch A and B
 * as A.cum_wt / (A.cum_wt + B.cum_wt) and B.cum_wt / (A.cum_wt + B.cum_wt),
 * respectively. That merge does preserve first-order probabilities, specifically
 * the probability proportional to size property, and like all other known merge
 * algorithms distorts second-order probabilities (co-occurrences). There are
 * pathological cases, most obvious with k=2 and A.cum_wt == B.cum_wt where that
 * approach will always take exactly 1 item from A and 1 from B, meaning the
 * co-occurrence rate for two items from either sketch is guaranteed to be 0.0.
 * 
 * With EBPPS, once an item is accepted into the sketch we no longer need to
 * track the item's weight, All accepted items are treated equally. As a result, we
 * can take inspiration from the reservoir sampling merge in the datasketches-java
 * library. We need to merge the smaller sketch into the larger one, swapping as
 * needed to ensure that, at which point we simply call update() with the items
 * in the smaller sketch with a weight of cum_wt / result_size -- we cannot just
 * divide by C since the number of items inserted will necesarily be an integer.
 * Merging smaller into larger is necessary to ensure that no item has a
 * contribution to C > 1.0.
 */

template<typename T, typename A>
void ebpps_sketch<T, A>::merge(ebpps_sketch<T>&& sk) {
  if (sk.get_cumulative_weight() == 0.0) return;
  else if (sk.get_cumulative_weight() > get_cumulative_weight()) {
    // need to swap this with sk to merge smaller into larger
    std::swap(*this, sk);
  }

  internal_merge(sk);
}

template<typename T, typename A>
void ebpps_sketch<T, A>::merge(const ebpps_sketch<T>& sk) {
  if (sk.get_cumulative_weight() == 0.0) return;
  else if (sk.get_cumulative_weight() > get_cumulative_weight()) {
    // need to swap this with sk to merge, so make a copy, swap,
    // and use that to merge
    ebpps_sketch sk_copy(sk);
    swap(*this, sk_copy);
    internal_merge(sk_copy);
  } else {
    internal_merge(sk);
  }
}

template<typename T, typename A>
template<typename O>
void ebpps_sketch<T, A>::internal_merge(O&& sk) {
  // TODO: remove after adding proper unit tests
  if (sk.cumulative_wt_ > cumulative_wt_)
    throw std::logic_error("internal_merge() trying to merge larger sketch into this");

  const ebpps_sample<T>& other_sample = sk.sample_;

  double final_cum_wt = cumulative_wt_ + sk.cumulative_wt_;
  double new_wt_max = std::max(wt_max_, sk.wt_max_);
  k_ = std::min(k_, sk.k_);
  uint64_t new_n = n_ + sk.n_;

  // Insert sk's items with the cumulative weight
  // split between the input items. We repeat the same process
  // for full items and the partial item, scaling the input
  // weight 
  // point value, we need to divide by the actual number of items
  // in the input. Consequently, we need to handle every 
  // item explicitly rather than probabilistically including
  // the partial item.
  double avg_wt = sk.get_cumulative_weight() / sk.get_c();
  auto items = other_sample.get_full_items();
  for (size_t i = 0; i < items.size(); ++i) {
    // new_wt_max is pre-computed
    double new_cum_wt = cumulative_wt_ + avg_wt;
    double new_rho = std::min(1.0 / new_wt_max, k_ / new_cum_wt);

    if (cumulative_wt_ > 0.0) {
      sample_.downsample(new_rho / rho_);
    }
  
    ebpps_sample<T,A> tmp(conditional_forward<O>(items[i]), new_rho * avg_wt, allocator_);

    sample_.merge(tmp);

    cumulative_wt_ = new_cum_wt;
    rho_ = new_rho;
  }
  // insert partial item with weight scaled by the fractional part of C
  if (other_sample.has_partial_item()) {
    double unused;
    double other_c_frac = std::modf(other_sample.get_c(), &unused);

    double new_cum_wt = cumulative_wt_ + (other_c_frac * avg_wt);
    double new_rho = std::min(1.0 / new_wt_max, k_ / new_cum_wt);

    if (cumulative_wt_ > 0.0)
      sample_.downsample(new_rho / rho_);
  
    ebpps_sample<T,A> tmp(conditional_forward<O>(other_sample.get_partial_item()), new_rho * other_c_frac * avg_wt, allocator_);

    sample_.merge(tmp);

    cumulative_wt_ = new_cum_wt;
    rho_ = new_rho;
  }

  // avoid numeric issues by setting cumulative weight to the
  // pre-computed value
  cumulative_wt_ = final_cum_wt;
  n_ = new_n;
}

/*
 * An empty sketch requires 8 bytes.
 *
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 * </pre>
 *
 * A non-empty sketch requires 48 bytes of preamble.
 *
 * The count of items seen is not used but preserved as the value seems like a useful
 * count to track.
 * 
 * <pre>
 * Long || Start Byte Adr:
 * Adr:
 *      ||       0        |    1   |    2   |    3   |    4   |    5   |    6   |    7   |
 *  0   || Preamble_Longs | SerVer | FamID  |  Flags |---------Max Res. Size (K)---------|
 *
 *      ||       8        |    9   |   10   |   11   |   12   |   13   |   14   |   15   |
 *  1   ||---------------------------Items Seen Count (N)--------------------------------|
 *
 *      ||      16        |   17   |   18   |   19   |   20   |   21   |   22   |   23   |
 *  2   ||----------------------------Cumulative Weight----------------------------------|
 *
 *      ||      24        |   25   |   26   |   27   |   28   |   29   |   30   |   31   |
 *  3   ||-----------------------------Max Item Weight-----------------------------------|
 *
 *      ||      32        |   33   |   34   |   35   |   36   |   37   |   38   |   39   |
 *  4   ||----------------------------------Rho------------------------------------------|
 *
 *      ||      40        |   41   |   42   |   43   |   44   |   45   |   46   |   47   |
 *  5   ||-----------------------------------C-------------------------------------------|
 *
 *      ||      40+                      |
 *  6+  ||  {Items Array}                |
 *      ||  {Optional Item (if needed)}  |
 * </pre>
 */

template<typename T, typename A>
template<typename SerDe>
size_t ebpps_sketch<T, A>::get_serialized_size_bytes(const SerDe& sd) const {
  if (is_empty()) { return PREAMBLE_LONGS_EMPTY << 3; }
  return (PREAMBLE_LONGS_FULL << 3) + sample_.get_serialized_size_bytes(sd);
}

template<typename T, typename A>
template<typename SerDe>
auto ebpps_sketch<T,A>::serialize(unsigned header_size_bytes, const SerDe& sd) const -> vector_bytes {
  const size_t size = header_size_bytes + get_serialized_size_bytes(sd);
  std::vector<uint8_t, A> bytes(size, 0, allocator_);
  uint8_t* ptr = bytes.data() + header_size_bytes;
  uint8_t* end_ptr = ptr + size;

  bool empty = is_empty();
  uint8_t prelongs = (empty ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_FULL);

  uint8_t flags = 0;
  if (empty) {
    flags |= EMPTY_FLAG_MASK;
  } else {
    flags |= sample_.has_partial_item() ? HAS_PARTIAL_ITEM_MASK : 0;
  }

  // first prelong
  uint8_t ser_ver = SER_VER;
  uint8_t family = FAMILY_ID;
  ptr += copy_to_mem(prelongs, ptr);
  ptr += copy_to_mem(ser_ver, ptr);
  ptr += copy_to_mem(family, ptr);
  ptr += copy_to_mem(flags, ptr);
  ptr += copy_to_mem(k_, ptr);

  // remaining preamble
  ptr += copy_to_mem(n_, ptr);
  ptr += copy_to_mem(cumulative_wt_, ptr);
  ptr += copy_to_mem(wt_max_, ptr);
  ptr += copy_to_mem(rho_, ptr);
  ptr += copy_to_mem(sample_.get_c(), ptr);

  // force inclusion of the partial item in the iterator,
  // which means we need to serialize one at a time rather
  // than being able to use an array
  auto it = sample_.begin(true);
  auto it_end = sample_.end();
  while (it != it_end)
    ptr += sd.serialize(ptr, end_ptr - ptr, it++, 1);

  return bytes;
}

template<typename T, typename A>
template<typename SerDe>
void ebpps_sketch<T,A>::serialize(std::ostream& os, const SerDe& sd) const {
  const bool empty = is_empty();

  const uint8_t prelongs = (empty ? PREAMBLE_LONGS_EMPTY : PREAMBLE_LONGS_FULL);

  uint8_t flags = 0;
  if (empty) {
    flags |= EMPTY_FLAG_MASK;
  } else {
    flags |= sample_.has_partial_item() ? HAS_PARTIAL_ITEM_MASK : 0;
  }

  // first prelong
  const uint8_t ser_ver = SER_VER;
  const uint8_t family = FAMILY_ID;
  write(os, prelongs);
  write(os, ser_ver);
  write(os, family);
  write(os, flags);
  write(os, k_);

  // remaining preamble
  write(os, n_);
  write(os, cumulative_wt_);
  write(os, wt_max_);
  write(os, rho_);
  write(os, sample_.get_c());

  // force inclusion of the partial item in the iterator,
  // which means we need to serialize one at a time rather
  // than being able to use an array
  auto it = sample_.begin(true);
  auto it_end = sample_.end();
  while (it != it_end)
    sd.serialize(os, it++, 1);

  if (!os.good()) throw std::runtime_error("error writing to std::ostream");
}

template<typename T, typename A>
template<typename SerDe>
ebpps_sketch<T,A> ebpps_sketch<T,A>::deserialize(const void* bytes, size_t size, const SerDe& sd, const A& allocator) {
  ensure_minimum_memory(size, 8);
  const char* ptr = static_cast<const char*>(bytes);
  const char* end_ptr = ptr + size;
  uint8_t prelongs;
  ptr += copy_from_mem(ptr, prelongs);
  uint8_t serial_version;
  ptr += copy_from_mem(ptr, serial_version);
  uint8_t family_id;
  ptr += copy_from_mem(ptr, family_id);
  uint8_t flags;
  ptr += copy_from_mem(ptr, flags);
  uint32_t k;
  ptr += copy_from_mem(ptr, k);

  check_preamble_longs(prelongs, flags);
  check_family_and_serialization_version(family_id, serial_version);
  ensure_minimum_memory(size, prelongs << 3);

  const bool empty = flags & EMPTY_FLAG_MASK;
  if (empty)
    return ebpps_sketch(k, allocator);

  uint64_t n;
  ptr += copy_from_mem(ptr, n);
  double cumulative_wt;
  ptr += copy_from_mem(ptr, cumulative_wt);
  double wt_max;
  ptr += copy_from_mem(ptr, wt_max);
  double rho;
  ptr += copy_from_mem(ptr, rho);
  double c;
  ptr += copy_from_mem(ptr, c);

  double c_int;
  double c_frac = std::modf(c, &c_int);
  bool has_partial = c_frac != 0.0;

  if (has_partial != (flags & HAS_PARTIAL_ITEM_MASK))
    throw std::runtime_error("sketch fails internal consistency check");

  std::vector<T, A> data(allocator);
  uint32_t num_full_items = static_cast<uint32_t>(c_int);
  if (num_full_items > 0) {
    data.reserve(num_full_items);
    ptr += sd.deserialize(ptr, end_ptr - ptr, data.data, num_full_items);
  }

  optional<T> partial_item;
  if (has_partial) {
    optional<T> tmp; // space to deserialize
    ptr += sd.deserialize(ptr, end_ptr - ptr, &*tmp, 1);
    // serde did not throw so place item and clean up
    partial_item.emplace(*tmp);
    (*tmp).~T();
  }

  auto sample = ebpps_sample<T,A>(data, partial_item, c, allocator);

  return ebpps_sketch(k, n, cumulative_wt, wt_max, rho, std::move(sample), allocator);
}

template<typename T, typename A>
template<typename SerDe>
ebpps_sketch<T,A> ebpps_sketch<T,A>::deserialize(std::istream& is, const SerDe& sd, const A& allocator) {
  const uint8_t prelongs = read<uint8_t>(is);
  const uint8_t ser_ver = read<uint8_t>(is);
  const uint8_t family = read<uint8_t>(is);
  const uint8_t flags = read<uint8_t>(is);
  const uint32_t k = read<uint32_t>(is);

  check_family_and_serialization_version(family, ser_ver);
  check_preamble_longs(prelongs, flags);

  const bool empty = (flags & EMPTY_FLAG_MASK);
  
  if (empty)
    return ebpps_sketch(k, allocator);

  const uint64_t n = read<uint64_t>(is);
  const double cumulative_wt = read<double>(is);
  const double wt_max = read<double>(is);
  const double rho = read<double>(is);
  const double c = read<double>(is);

  double c_int;
  double c_frac = std::modf(c, &c_int);
  bool has_partial = c_frac != 0.0;

  if (has_partial != (flags & HAS_PARTIAL_ITEM_MASK))
    throw std::runtime_error("sketch fails internal consistency check");

  std::vector<T, A> data(allocator);
  uint32_t num_full_items = static_cast<uint32_t>(c_int);
  if (num_full_items > 0) {
    data.reserve(num_full_items);
    sd.deserialize(is, data.data, num_full_items);
  }

  optional<T> partial_item;
  if (has_partial) {
    optional<T> tmp; // space to deserialize
    sd.deserialize(is, &*tmp, 1);
    // serde did not throw so place item and clean up
    partial_item.emplace(*tmp);
    (*tmp).~T();
  }

  if (!is.good()) throw std::runtime_error("error reading from std::istream");

  auto sample = ebpps_sample<T,A>(data, partial_item, c, allocator);

  return ebpps_sketch(k, n, cumulative_wt, wt_max, rho, std::move(sample), allocator);
}

template<typename T, typename A>
void ebpps_sketch<T, A>::check_preamble_longs(uint8_t preamble_longs, uint8_t flags) {
  const bool is_empty(flags & EMPTY_FLAG_MASK);
  
  if (is_empty) {
    if (preamble_longs != PREAMBLE_LONGS_EMPTY) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_EMPTY) + " for an empty sketch. Found: "
        + std::to_string(preamble_longs));
    }
    if (flags & HAS_PARTIAL_ITEM_MASK) {
      throw std::invalid_argument("Possible corruption: Empty sketch must not "
        "contain indications of the presence of any item");
    }
  } else {
    if (preamble_longs != PREAMBLE_LONGS_FULL) {
      throw std::invalid_argument("Possible corruption: Preamble longs must be "
        + std::to_string(PREAMBLE_LONGS_FULL)
        + " for a non-empty sketch. Found: " + std::to_string(preamble_longs));
    }
  }
}

template<typename T, typename A>
void ebpps_sketch<T, A>::check_family_and_serialization_version(uint8_t family_id, uint8_t ser_ver) {
  if (family_id == FAMILY_ID) {
    if (ser_ver != SER_VER) {
      throw std::invalid_argument("Possible corruption: EBPPS serialization version must be "
        + std::to_string(SER_VER) + ". Found: " + std::to_string(ser_ver));
    }
    return;
  }

  throw std::invalid_argument("Possible corruption: EBPPS Sketch family id must be "
    + std::to_string(FAMILY_ID) + ". Found: " + std::to_string(family_id));
}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator ebpps_sketch<T, A>::begin() const {
  return sample_.begin();
}

template<typename T, typename A>
typename ebpps_sample<T, A>::const_iterator ebpps_sketch<T, A>::end() const {
  return sample_.end();
}

} // namespace datasketches

#endif // _EBPPS_SKETCH_IMPL_HPP_
