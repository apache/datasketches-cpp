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

#ifndef _HLL_HPP_
#define _HLL_HPP_

#include "HllUtil.hpp"
#include "PairIterator.hpp"

#include <memory>
#include <iostream>

namespace datasketches {

// The different types of HLL sketches
enum target_hll_type {
    HLL_4,
    HLL_6,
    HLL_8
};

template<typename A>
class HllSketchImpl;

template<typename A>
class hll_union_alloc;

using byte_ptr_with_deleter = std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>;

template<typename A = std::allocator<char> >
class hll_sketch_alloc final {
  public:
    explicit hll_sketch_alloc(int lg_config_k, target_hll_type tgt_type = HLL_4, bool start_full_size = false);
    static hll_sketch_alloc deserialize(std::istream& is);
    static hll_sketch_alloc deserialize(const void* bytes, size_t len);
    hll_sketch_alloc(const hll_sketch_alloc<A>& that);
    hll_sketch_alloc(const hll_sketch_alloc<A>& that, target_hll_type tgt_type);
    hll_sketch_alloc(hll_sketch_alloc<A>&& that) noexcept;

    ~hll_sketch_alloc();

    hll_sketch_alloc operator=(const hll_sketch_alloc<A>& other);
    hll_sketch_alloc operator=(hll_sketch_alloc<A>&& other);

    void reset();
    
    std::pair<byte_ptr_with_deleter, const size_t> serialize_compact(unsigned header_size_bytes = 0) const;
    std::pair<byte_ptr_with_deleter, const size_t> serialize_updatable() const;
    void serialize_compact(std::ostream& os) const;
    void serialize_updatable(std::ostream& os) const;
    
    std::ostream& to_string(std::ostream& os,
                            bool summary = true,
                            bool detail = false,
                            bool aux_detail = false,
                            bool all = false) const;
    std::string to_string(bool summary = true,
                          bool detail = false,
                          bool aux_detail = false,
                          bool all = false) const;                                    

    void update(const std::string& datum);
    void update(uint64_t datum);
    void update(uint32_t datum);
    void update(uint16_t datum);
    void update(uint8_t datum);
    void update(int64_t datum);
    void update(int32_t datum);
    void update(int16_t datum);
    void update(int8_t datum);
    void update(double datum);
    void update(float datum);
    void update(const void* data, size_t length_bytes);

    double get_estimate() const;
    double get_composite_estimate() const;
    double get_lower_bound(int num_std_dev) const;
    double get_upper_bound(int num_std_dev) const;

    int get_lg_config_k() const;
    target_hll_type get_target_type() const;

    bool is_compact() const;
    bool is_empty() const;

    int get_updatable_serialization_bytes() const;
    int get_compact_serialization_bytes() const;

    /**
     * Returns the maximum size in bytes that this sketch can grow to given lg_config_k.
     * However, for the HLL_4 sketch type, this value can be exceeded in extremely rare cases.
     * If exceeded, it will be larger by only a few percent.
     *
     * @param lg_config_k The Log2 of K for the target HLL sketch. This value must be
     * between 4 and 21 inclusively.
     * @param tgt_type the desired Hll type
     * @return the maximum size in bytes that this sketch can grow to.
     */
    static int get_max_updatable_serialization_bytes(int lg_k, target_hll_type tgt_type);
    static double get_rel_err(bool upper_bound, bool unioned,
                              int lg_config_k, int num_std_dev);

    pair_iterator_with_deleter<A> get_iterator() const;

  private:
    explicit hll_sketch_alloc(HllSketchImpl<A>* that);

    void coupon_update(int coupon);

    std::string type_as_string() const;
    std::string mode_as_string() const;

    CurMode get_current_mode() const;
    int get_serialization_version() const;
    bool is_out_of_order_flag() const;
    bool is_estimation_mode() const;

    typedef typename std::allocator_traits<A>::template rebind_alloc<hll_sketch_alloc> AllocHllSketch;
    friend AllocHllSketch;

    HllSketchImpl<A>* sketch_impl;
    friend hll_union_alloc<A>;
};

template<typename A = std::allocator<char> >
class hll_union_alloc {
  public:
    explicit hll_union_alloc(int lg_max_k);

    static hll_union_alloc deserialize(std::istream& is);
    static hll_union_alloc deserialize(const void* bytes, size_t len);

    double get_estimate() const;
    double get_composite_estimate() const;
    double get_lower_bound(int num_std_dev) const;
    double get_upper_bound(int num_std_dev) const;

    int get_compact_serialization_bytes() const;
    int get_updatable_serialization_bytes() const;
    int get_lg_config_k() const;

    target_hll_type get_target_type() const;
    bool is_compact() const;
    bool is_empty() const;

    void reset();

    hll_sketch_alloc<A> get_result(target_hll_type tgt_type = HLL_4) const;

    std::pair<byte_ptr_with_deleter, const size_t> serialize_compact() const;
    std::pair<byte_ptr_with_deleter, const size_t> serialize_updatable() const;
    void serialize_compact(std::ostream& os) const;
    void serialize_updatable(std::ostream& os) const;

    std::ostream& to_string(std::ostream& os,
                            bool summary = true,
                            bool detail = false,
                            bool aux_Detail = false,
                            bool all = false) const;
    std::string to_string(bool summary = true,
                          bool detail = false,
                          bool aux_detail = false,
                          bool all = false) const;                                    

    void update(const hll_sketch_alloc<A>& sketch);
    void update(const std::string& datum);
    void update(uint64_t datum);
    void update(uint32_t datum);
    void update(uint16_t datum);
    void update(uint8_t datum);
    void update(int64_t datum);
    void update(int32_t datum);
    void update(int16_t datum);
    void update(int8_t datum);
    void update(double datum);
    void update(float datum);
    void update(const void* data, size_t length_bytes);

    static int get_max_serialization_bytes(int lg_k);
    static double get_rel_err(bool upper_bound, bool unioned,
                              int lg_config_k, int num_std_dev);

  private:

   /**
    * Union the given source and destination sketches. This static method examines the state of
    * the current internal gadget and the incoming sketch and determines the optimum way to
    * perform the union. This may involve swapping, down-sampling, transforming, and / or
    * copying one of the arguments and may completely replace the internals of the union.
    *
    * @param incoming_impl the given incoming sketch, which may not be modified.
    * @param lg_max_k the maximum value of log2 K for this union.
    * //@return the union of the two sketches in the form of the internal HllSketchImpl, which for
    * //the union is always in HLL_8 form.
    */
    void union_impl(HllSketchImpl<A>* incoming_impl, int lg_max_k);

    static HllSketchImpl<A>* copy_or_downsample(HllSketchImpl<A>* src_impl, int tgt_lg_k);

    void coupon_update(int coupon);

    CurMode get_current_mode() const;
    int get_serialization_version() const;
    bool is_out_of_order_flag() const;
    bool is_estimation_mode() const;

    // calls couponUpdate on sketch, freeing the old sketch upon changes in CurMode
    static HllSketchImpl<A>* leak_free_coupon_update(HllSketchImpl<A>* impl, int coupon);

    int lg_max_k;
    hll_sketch_alloc<A> gadget;

};

template<typename A>
static std::ostream& operator<<(std::ostream& os, const hll_sketch_alloc<A>& sketch);

template<typename A>
static std::ostream& operator<<(std::ostream& os, const hll_union_alloc<A>& union_in);

// aliases with default allocator for convenience
typedef hll_sketch_alloc<> hll_sketch;
typedef hll_union_alloc<> hll_union;

} // namespace datasketches

#include "hll.private.hpp"

#endif // _HLL_HPP_
