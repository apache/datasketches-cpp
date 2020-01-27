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

#ifndef _VAR_OPT_SKETCH_HPP_
#define _VAR_OPT_SKETCH_HPP_

#include "serde.hpp"

#include <vector>
#include <iterator>

namespace datasketches {

template<typename A> using AllocU8 = typename std::allocator_traits<A>::template rebind_alloc<uint8_t>;

int num_allocs = 0;

/**
 * author Kevin Lang 
 * author Jon Malkin
 */
template <typename T, typename S = serde<T>, typename A = std::allocator<T>>
class var_opt_sketch {

  public:
    enum resize_factor { X1 = 0, X2, X4, X8 };
    static const resize_factor DEFAULT_RESIZE_FACTOR = X8;

    var_opt_sketch(uint32_t k, resize_factor rf = DEFAULT_RESIZE_FACTOR);
    static var_opt_sketch<T,S,A> deserialize(std::istream& is);
    static var_opt_sketch<T,S,A> deserialize(const void* bytes, size_t size);

    virtual ~var_opt_sketch();

    void update(const T& item, double weight=1.0);
    //void update(T&& item, double weight=1.0);

    uint32_t get_k() const;
    uint64_t get_n() const;
    uint32_t get_num_samples() const;
    
    bool is_empty() const;
    void reset();

    // version for fixed-size arithmetic types (integer, floating point)
    template<typename TT = T, typename std::enable_if<std::is_arithmetic<TT>::value, int>::type = 0>
    size_t get_serialized_size_bytes() const;

    // version for all other types
    template<typename TT = T, typename std::enable_if<!std::is_arithmetic<TT>::value, int>::type = 0>
    size_t get_serialized_size_bytes() const;

    std::vector<uint8_t, AllocU8<A>> serialize(unsigned header_size_bytes = 0) const;
    void serialize(std::ostream& os) const;
 
    std::ostream& to_stream(std::ostream& os) const;
    std::string to_string() const;

    //estimate_subset_sum()

  private:
    typedef typename std::allocator_traits<A>::template rebind_alloc<double> AllocDouble;
    typedef typename std::allocator_traits<A>::template rebind_alloc<bool> AllocBool;

    static const uint32_t MIN_LG_ARR_ITEMS = 4;

    static const uint8_t PREAMBLE_LONGS_EMPTY  = 1;
    static const uint8_t PREAMBLE_LONGS_WARMUP = 3;
    static const uint8_t PREAMBLE_LONGS_FULL   = 4;
    static const uint8_t SER_VER = 2;
    static const uint8_t FAMILY  = 12;
    static const uint8_t EMPTY_FLAG_MASK  = 4;
    static const uint8_t GADGET_FLAG_MASK = 128;

    // TODO: should probably rearrange a bit to minimize gaps once aligned
    uint32_t k_;                    // max size of sketch, in items

    uint32_t h_;                    // number of items in heap
    uint32_t m_;                    // number of items in middle region
    uint32_t r_;                    // number of items in reservoir-like region

    uint64_t n_;                    // total number of items processed by sketch
    double total_wt_r_;             // total weight of items in reservoir-like area

    const resize_factor rf_;        // resize factor

    uint32_t curr_items_alloc_;     // currently allocated array size
    bool filled_data_;              // true if we've explciitly set all entries in data_

    T* data_;                       // stored sampled items
    double* weights_;               // weights for sampled items

    // The next two fields are hidden from the user because they are part of the state of the
    // unioning algorithm, NOT part of a varopt sketch, or even of a varopt "gadget" (our name for
    // the potentially invalid sketch that is maintained by the unioning algorithm). It would make
    // more sense logically for these fields to be declared in the unioning object (whose entire
    // purpose is storing the state of the unioning algorithm) but for reasons of programming
    // convenience we are currently declaring them here. However, that could change in the future.

    // Following int is:
    //  1. Zero (for a varopt sketch)
    //  2. Count of marked items in H region, if part of a unioning algo's gadget
    uint32_t num_marks_in_h_;

    // The following array is absent in a varopt sketch, and notionally present in a gadget
    // (although it really belongs in the unioning object). If the array were to be made explicit,
    // some additional coding would need to be done to ensure that all of the necessary data motion
    // occurs and is properly tracked.
    bool* marks_;

    var_opt_sketch(uint32_t k, resize_factor rf, bool is_gadget);
    var_opt_sketch(uint32_t k, resize_factor rf, bool is_gadget, uint8_t preamble_longs, std::istream& is);
    var_opt_sketch(uint32_t k, resize_factor rf, bool is_gadget, uint8_t preamble_longs, const void* bytes, size_t size);

    // internal-use-only updates
    void update(const T& item, double weight, bool mark);
    void update_warmup_phase(const T& item, double weight, bool mark);
    void update_light(const T& item, double weight, bool mark);
    void update_heavy_r_eq1(const T& item, double weight, bool mark);
    void update_heavy_general(const T& item, double weight, bool mark);

    double get_tau() const;
    double peek_min() const;
    bool is_marked(int idx) const;
    
    int pick_random_slot_in_r() const;
    int choose_delete_slot(double wt_cand, int num_cand) const;
    int choose_weighted_delete_slot(double wt_cand, int num_cand) const;

    void transition_from_warmup();
    void convert_to_heap();
    void restore_towards_leaves(int slot_in);
    void restore_towards_root(int slot_in);
    void push(const T& item, double wt, bool mark);
    void pop_min_to_m_region();
    void grow_candidate_set(double wt_cands, int num_cands);    
    void decrease_k_by_1();
    void force_set_k(int k); // used to resolve union gadget into sketch
    void downsample_candidate_set(double wt_cands, int num_cands);
    void swap_values(int src, int dst);
    void grow_data_arrays();
    void allocate_data_arrays(uint32_t tgt_size, bool use_marks);

    // validation
    static void check_preamble_longs(uint8_t preamble_longs, uint8_t flags);
    static void check_family_and_serialization_version(uint8_t family_id, uint8_t ser_ver);
    
    // things to move to common utils and share among sketches
    static int get_adjusted_size(int max_size, int resize_target);
    static int starting_sub_multiple(int lg_target, int lg_rf, int lg_min);
    static bool is_power_of_2(uint32_t v);
    static uint32_t to_log_2(uint32_t v);
    static uint32_t count_trailing_zeros(uint32_t v);
    static uint32_t ceiling_power_of_2(uint32_t n);
    static int next_int(int max_value);
    static double next_double_exclude_zero();

    class const_iterator;
    const_iterator begin() const;
    const_iterator end() const;
};

template<typename T, typename S, typename A>
class var_opt_sketch<T, S, A>::const_iterator: public std::iterator<std::input_iterator_tag, T> {
public:
  friend class var_opt_sketch<T, S, A>;
  const_iterator(const const_iterator& other);
  const_iterator& operator++();
  const_iterator& operator++(int);
  bool operator==(const const_iterator& other) const;
  bool operator!=(const const_iterator& other) const;
  const std::pair<const T&, const double> operator*() const;
private:
  const T* items;
  const double* weights;
  const uint32_t h_count;
  const uint32_t r_count;
  const double r_item_wt;
  uint32_t index;
  const_iterator(const T* items, const double* weights, const uint32_t h_count, const uint32_t r_count,
                 const double total_wt_r, bool use_end=false);
};

} // namespace datasketches

#include "var_opt_sketch_impl.hpp"

#endif // _VAR_OPT_SKETCH_HPP_