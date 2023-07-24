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

#ifndef _EBPPS_SAMPLE_HPP_
#define _EBPPS_SAMPLE_HPP_

#include "common_defs.hpp"
#include "optional.hpp"

#include <memory>
#include <vector>

namespace datasketches {

template<
  typename T,
  typename A = std::allocator<T>
>
class ebpps_sample {
  public:
    explicit ebpps_sample(uint32_t k, const A& allocator = A());

    ebpps_sample(const T& item, double theta, const A& allocator = A());
    ebpps_sample(T&& item, double theta, const A& allocator = A());

    void reset();
    void downsample(double theta);

    template<typename FwdSample>
    void merge(FwdSample&& other);

    using result_type = std::vector<T, A>;
    result_type get_sample() const;

    double get_c() const;
    bool has_partial() const;
    
    string<A> to_string() const;

    class const_iterator;

    /**
     * Iterator pointing to the first item in the sample.
     * If the sample is empty, the returned iterator must not be dereferenced or incremented
     * @return iterator pointing to the first item in the sample
     */
    const_iterator begin() const;

    /**
     * Iterator pointing to the past-the-end item in the sample.
     * The past-the-end item is the hypothetical item that would follow the last item.
     * It does not point to any item, and must not be dereferenced or incremented.
     * @return iterator pointing to the past-the-end item in the sample
     */
    const_iterator end() const;

  private:
    A allocator_;
    double c_;                      // Current sample size, including fractional part
    optional<T> partial_item_;      // a sample item corresponding to a partial weight
    std::vector<T> data_;           // stored sampled items

    template<typename FwdItem>
    inline void set_partial(FwdItem&& item);
    void swap_with_partial();
    void move_one_to_partial();
    void subsample(uint32_t num_samples);

    const T& get_partial_item() const;

    static inline uint32_t random_idx(uint32_t max);
    static inline double next_double();

    // TODO: remove me
    void validate_sample() const;

    friend class const_iterator;
};

template<typename T, typename A>
class ebpps_sample<T, A>::const_iterator {
public:
  using iterator_category = std::input_iterator_tag;
  using value_type = const T&;
  using difference_type = void;
  using pointer = const return_value_holder<value_type>;
  using reference = const value_type;

  const_iterator(const const_iterator& other);
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  reference operator*() const;
  pointer operator->() const;

private:
  static const size_t PARTIAL_IDX = -1;

  // default iterator over sample
  const_iterator(const ebpps_sample<T, A>* sample, bool force_partial = false);

  const ebpps_sample<T, A>* sample_;
  size_t idx_;
  bool use_partial_;

  friend class ebpps_sample;
};

} // namespace datasketches

#include "ebpps_sample_impl.hpp"

#endif // _EBPPS_SAMPLE_HPP_
