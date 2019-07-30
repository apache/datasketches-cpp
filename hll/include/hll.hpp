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
enum TgtHllType {
    HLL_4,
    HLL_6,
    HLL_8
};

template<typename A>
class HllSketchImpl;

template<typename A>
class HllUnion;

using byte_ptr_with_deleter = std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>;

template<typename A = std::allocator<char> >
class HllSketch final {
  public:
    explicit HllSketch(int lgConfigK, TgtHllType tgtHllType = HLL_4, bool startFullSize = false);
    static HllSketch deserialize(std::istream& is);
    static HllSketch deserialize(const void* bytes, size_t len);
    HllSketch(const HllSketch<A>& that);
    HllSketch(const HllSketch<A>& that, TgtHllType tgtHllType);
    HllSketch(HllSketch<A>&& that) noexcept;

    ~HllSketch();

    HllSketch operator=(const HllSketch<A>& other);
    HllSketch operator=(HllSketch<A>&& other);

    void reset();
    
    std::pair<byte_ptr_with_deleter, const size_t> serializeCompact(unsigned header_size_bytes = 0) const;
    std::pair<byte_ptr_with_deleter, const size_t> serializeUpdatable() const;
    void serializeCompact(std::ostream& os) const;
    void serializeUpdatable(std::ostream& os) const;
    
    std::ostream& to_string(std::ostream& os,
                            bool summary = true,
                            bool detail = false,
                            bool auxDetail = false,
                            bool all = false) const;
    std::string to_string(bool summary = true,
                          bool detail = false,
                          bool auxDetail = false,
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
    void update(const void* data, size_t lengthBytes);

    double getEstimate() const;
    double getCompositeEstimate() const;
    double getLowerBound(int numStdDev) const;
    double getUpperBound(int numStdDev) const;

    int getLgConfigK() const;
    TgtHllType getTgtHllType() const;

    bool isCompact() const;
    bool isEmpty() const;

    int getUpdatableSerializationBytes() const;
    int getCompactSerializationBytes() const;

    /**
     * Returns the maximum size in bytes that this sketch can grow to given lgConfigK.
     * However, for the HLL_4 sketch type, this value can be exceeded in extremely rare cases.
     * If exceeded, it will be larger by only a few percent.
     *
     * @param lgConfigK The Log2 of K for the target HLL sketch. This value must be
     * between 4 and 21 inclusively.
     * @param tgtHllType the desired Hll type
     * @return the maximum size in bytes that this sketch can grow to.
     */
    static int getMaxUpdatableSerializationBytes(int lgK, TgtHllType tgtHllType);
    static double getRelErr(bool upperBound, bool unioned,
                            int lgConfigK, int numStdDev);

    //std::unique_ptr<PairIterator<A>> getIterator() const;
    PairIterator_with_deleter<A> getIterator() const;

  private:
    explicit HllSketch(HllSketchImpl<A>* that);

    void couponUpdate(int coupon);

    std::string typeAsString() const;
    std::string modeAsString() const;

    CurMode getCurrentMode() const;
    int getSerializationVersion() const;
    bool isOutOfOrderFlag() const;
    bool isEstimationMode() const;

    typedef typename std::allocator_traits<A>::template rebind_alloc<HllSketch> AllocHllSketch;
    friend AllocHllSketch;

    HllSketchImpl<A>* hllSketchImpl;
    friend HllUnion<A>;
};

template<typename A = std::allocator<char> >
class HllUnion {
  public:
    explicit HllUnion(int lgMaxK);

    static HllUnion deserialize(std::istream& is);
    static HllUnion deserialize(const void* bytes, size_t len);

    double getEstimate() const;
    double getCompositeEstimate() const;
    double getLowerBound(int numStdDev) const;
    double getUpperBound(int numStdDev) const;

    int getCompactSerializationBytes() const;
    int getUpdatableSerializationBytes() const;
    int getLgConfigK() const;

    TgtHllType getTgtHllType() const;
    bool isCompact() const;
    bool isEmpty() const;

    void reset();

    HllSketch<A> getResult(TgtHllType tgtHllType = HLL_4) const;

    std::pair<byte_ptr_with_deleter, const size_t> serializeCompact() const;
    std::pair<byte_ptr_with_deleter, const size_t> serializeUpdatable() const;
    void serializeCompact(std::ostream& os) const;
    void serializeUpdatable(std::ostream& os) const;

    std::ostream& to_string(std::ostream& os,
                            bool summary = true,
                            bool detail = false,
                            bool auxDetail = false,
                            bool all = false) const;
    std::string to_string(bool summary = true,
                          bool detail = false,
                          bool auxDetail = false,
                          bool all = false) const;                                    

    void update(const HllSketch<A>& sketch);
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
    void update(const void* data, size_t lengthBytes);

    static int getMaxSerializationBytes(int lgK);
    static double getRelErr(bool upperBound, bool unioned,
                            int lgConfigK, int numStdDev);

  private:

   /**
    * Union the given source and destination sketches. This static method examines the state of
    * the current internal gadget and the incoming sketch and determines the optimum way to
    * perform the union. This may involve swapping, down-sampling, transforming, and / or
    * copying one of the arguments and may completely replace the internals of the union.
    *
    * @param incomingImpl the given incoming sketch, which may not be modified.
    * @param gadgetImpl the given gadget sketch, which must have a target of HLL_8 and may be
    * modified.
    * @param lgMaxK the maximum value of log2 K for this union.
    * //@return the union of the two sketches in the form of the internal HllSketchImpl, which for
    * //the union is always in HLL_8 form.
    */
    void unionImpl(HllSketchImpl<A>* incomingImpl, int lgMaxK);

    static HllSketchImpl<A>* copyOrDownsampleHll(HllSketchImpl<A>* srcImpl, int tgtLgK);

    void couponUpdate(int coupon);

    CurMode getCurrentMode() const;
    int getSerializationVersion() const;
    bool isOutOfOrderFlag() const;
    bool isEstimationMode() const;

    // calls couponUpdate on sketch, freeing the old sketch upon changes in CurMode
    static HllSketchImpl<A>* leakFreeCouponUpdate(HllSketchImpl<A>* impl, int coupon);

    int lgMaxK;
    HllSketch<A> gadget;

};

template<typename A>
static std::ostream& operator<<(std::ostream& os, const HllSketch<A>& sketch);

template<typename A>
static std::ostream& operator<<(std::ostream& os, const HllUnion<A>& hllUnion);

// aliases with default allocator for convenience
typedef HllSketch<> hll_sketch;
typedef HllUnion<> hll_union;

} // namespace datasketches

#include "hll.private.hpp"

#endif // _HLL_HPP_
