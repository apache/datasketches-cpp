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

#ifndef _TDIGEST_HPP_
#define _TDIGEST_HPP_

#include <type_traits>
#include <limits>

#include "common_defs.hpp"

namespace datasketches {

// this is equivalent of K_2 (default) in the Java implementation mentioned below
struct scale_function {
  double k(double q, double normalizer) const {
    return limit([normalizer] (double q) { return std::log(q / (1 - q)) * normalizer; }, q, 1e-15, 1 - 1e-15);
  }
  double q(double k, double normalizer) const {
    const double w = std::exp(k / normalizer);
    return w / (1 + w);
  }
  double max(double q, double normalizer) const {
    return q * (1 - q) / normalizer;
  }
  double normalizer(double compression, double n) const {
    return compression / z(compression, n);
  }
  double z(double compression, double n) const {
    return 4 * std::log(n / compression) + 24;
  }

  template<typename Func>
  double limit(Func f, double x, double low, double high) const {
    if (x < low) return f(low);
    if (x > high) return f(high);
    return f(x);
  }
};

// forward declaration
template <typename T, typename Allocator = std::allocator<T>> class tdigest;

/// TDigest float sketch
using tdigest_float = tdigest<float>;
/// TDigest double sketch
using tdigest_double = tdigest<double>;

/**
 * t-Digest for estimating quantiles and ranks.
 * This implementation is based on the following paper:
 * Ted Dunning, Otmar Ertl. Extremely Accurate Quantiles Using t-Digests
 * and the following implementation in Java:
 * https://github.com/tdunning/t-digest
 * This implementation is similar to MergingDigest in the above Java implementation
 */
template <typename T, typename Allocator>
class tdigest {
  static_assert(std::is_floating_point<T>::value, "Floating-point type expected");
  static_assert(std::numeric_limits<T>::is_iec559, "IEEE 754 compatibility required");
public:
  using value_type = T;
  using allocator_type = Allocator;

  static const bool USE_ALTERNATING_SORT = true;
  static const bool USE_TWO_LEVEL_COMPRESSION = true;
  static const bool USE_WEIGHT_LIMIT = true;

  class centroid {
  public:
    centroid(T value, uint64_t weight): mean_(value), weight_(weight) {}
    void add(const centroid& other) {
      weight_ += other.weight_;
      mean_ += (other.mean_ - mean_) * other.weight_ / weight_;
    }
    T get_mean() const { return mean_; }
    uint64_t get_weight() const { return weight_; }
  private:
    T mean_;
    uint64_t weight_;
  };
  using vector_centroid = std::vector<centroid, typename std::allocator_traits<Allocator>::template rebind_alloc<centroid>>;

  struct centroid_cmp {
    centroid_cmp(bool reverse): reverse_(reverse) {}
    bool operator()(const centroid& a, const centroid& b) const {
      if (a.get_mean() < b.get_mean()) return !reverse_;
      return reverse_;
    }
    bool reverse_;
  };

  /**
   * Constructor
   * @param k affects the size of the sketch and its estimation error
   * @param allocator used to allocate memory
   */
  explicit tdigest(uint16_t k = 100, const Allocator& allocator = Allocator());

  /**
   * Update this t-Digest with the given value
   * @param value to update the t-Digest with
   */
  void update(T value);

  /**
   * Merge the given t-Digest into this one
   * @param other t-Digest to merge
   */
  void merge(tdigest& other);

  /**
   * Process buffered values and merge centroids if needed
   */
  void compress();

  /**
   * @return true if t-Digest has not seen any data
   */
  bool is_empty() const;

  /**
   * @return minimum value seen by t-Digest
   */
  T get_min_value() const;

  /**
   * @return maximum value seen by t-Digest
   */
  T get_max_value() const;

  /**
   * @return total weight
   */
  uint64_t get_total_weight() const;

  /**
   * Compute approximate normalized rank of the given value.
   * @param value to be ranked
   * @return normalized rank (from 0 to 1 inclusive)
   */
  double get_rank(T value) const;

  /**
   * Compute approximate quantile value corresponding to the given normalized rank
   * @param rank normalized rank (from 0 to 1 inclusive)
   * @return quantile value corresponding to the given rank
   */
  T get_quantile(double rank) const;

  /**
   * @return parameter k (compression) that was used to configure this t-Digest
   */
  uint16_t get_k() const;

  /**
   * Human-readable summary of this t-Digest as a string
   * @param print_centroids if true append the list of centroids with weights
   * @return summary of this t-Digest
   */
  string<Allocator> to_string(bool print_centroids = false) const;

private:
  Allocator allocator_;
  uint16_t k_;
  uint16_t internal_k_;
  uint32_t merge_count_;
  T min_;
  T max_;
  size_t centroids_capacity_;
  vector_centroid centroids_;
  uint64_t total_weight_;
  size_t buffer_capacity_;
  vector_centroid buffer_;
  uint64_t buffered_weight_;

  void merge_new_values();
  void merge_new_values(bool force, uint16_t k);
  void merge_new_values(vector_centroid& centroids, uint64_t weight, uint16_t k, bool reverse);
};

} /* namespace datasketches */

#include "tdigest_impl.hpp"

#endif // _TDIGEST_HPP_
