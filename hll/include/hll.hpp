/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLL_HPP_
#define _HLL_HPP_

#include "HllUtil.hpp"

#include <memory>
#include <iostream>

namespace datasketches {

// The different types of HLL sketches
enum TgtHllType {
    HLL_4,
    HLL_6,
    HLL_8
};

class HllSketchImpl;
class PairIterator;

template<typename A>
class HllUnion;

template<typename A = std::allocator<char> >
class HllSketch final {
  public:
    explicit HllSketch(int lgConfigK, TgtHllType tgtHllType = HLL_4);
    static HllSketch deserialize(std::istream& is);
    static HllSketch deserialize(const void* bytes, size_t len);
    HllSketch(const HllSketch<A>& that);

    ~HllSketch();

    HllSketch operator=(HllSketch<A>& other);
    HllSketch operator=(HllSketch<A>&& other);
    HllSketch copy() const;
    HllSketch* copyPtr() const;
    HllSketch copyAs(TgtHllType tgtHllType) const;

    void reset();
    
    std::pair<std::unique_ptr<uint8_t[]>, const size_t> serializeCompact() const;
    std::pair<std::unique_ptr<uint8_t[]>, const size_t> serializeUpdatable() const;
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

    std::unique_ptr<PairIterator> getIterator() const;

  private:
    explicit HllSketch(HllSketchImpl* that);

    void couponUpdate(int coupon);

    std::string typeAsString() const;
    std::string modeAsString() const;

    CurMode getCurrentMode() const;
    int getSerializationVersion() const;
    bool isOutOfOrderFlag() const;
    bool isEstimationMode() const;

    typedef typename std::allocator_traits<A>::template rebind_alloc<HllSketch> AllocHllSketch;
    friend AllocHllSketch;

    HllSketchImpl* hllSketchImpl;
    friend HllUnion<A>;
};

template<typename A = std::allocator<char> >
class HllUnion {
  public:
    //static HllUnion newInstance(int lgMaxK);
    explicit HllUnion(int lgMaxK);
    explicit HllUnion(HllSketch<A>& sketch);
    HllUnion(const HllUnion<A>& that);

    static HllUnion deserialize(std::istream& is);
    static HllUnion deserialize(const void* bytes, size_t len);

    virtual ~HllUnion();

    HllUnion operator=(HllUnion<A>& other);
    HllUnion operator=(HllUnion<A>&& other);

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getLowerBound(int numStdDev) const;
    virtual double getUpperBound(int numStdDev) const;

    virtual int getCompactSerializationBytes() const;
    virtual int getUpdatableSerializationBytes() const;
    virtual int getLgConfigK() const;

    virtual TgtHllType getTgtHllType() const;
    virtual bool isCompact() const;
    virtual bool isEmpty() const;

    virtual void reset();

    virtual HllSketch<A> getResult(TgtHllType tgtHllType = HLL_4) const;

    virtual std::pair<std::unique_ptr<uint8_t[]>, const size_t> serializeCompact() const;
    virtual std::pair<std::unique_ptr<uint8_t[]>, const size_t> serializeUpdatable() const;
    virtual void serializeCompact(std::ostream& os) const;
    virtual void serializeUpdatable(std::ostream& os) const;

    virtual std::ostream& to_string(std::ostream& os,
                                    bool summary = true,
                                    bool detail = false,
                                    bool auxDetail = false,
                                    bool all = false) const;
    virtual std::string to_string(bool summary = true,
                                  bool detail = false,
                                  bool auxDetail = false,
                                  bool all = false) const;                                    

    virtual void update(const HllSketch<A>& sketch);
    virtual void update(const std::string& datum);
    virtual void update(uint64_t datum);
    virtual void update(uint32_t datum);
    virtual void update(uint16_t datum);
    virtual void update(uint8_t datum);
    virtual void update(int64_t datum);
    virtual void update(int32_t datum);
    virtual void update(int16_t datum);
    virtual void update(int8_t datum);
    virtual void update(double datum);
    virtual void update(float datum);
    virtual void update(const void* data, size_t lengthBytes);

    static int getMaxSerializationBytes(int lgK);
    static double getRelErr(bool upperBound, bool unioned,
                            int lgConfigK, int numStdDev);

  private:
    typedef typename std::allocator_traits<A>::template rebind_alloc<HllUnion> AllocHllUnion;

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
    void unionImpl(HllSketchImpl* incomingImpl, int lgMaxK);

    static HllSketchImpl* copyOrDownsampleHll(HllSketchImpl* srcImpl, int tgtLgK);

    void couponUpdate(int coupon);

    CurMode getCurrentMode() const;
    int getSerializationVersion() const;
    bool isOutOfOrderFlag() const;
    bool isEstimationMode() const;

    // calls couponUpdate on sketch, freeing the old sketch upon changes in CurMode
    static HllSketchImpl* leakFreeCouponUpdate(HllSketchImpl* impl, int coupon);

    int lgMaxK;
    HllSketch<A>* gadget;

};

template<typename A>
static std::ostream& operator<<(std::ostream& os, const HllSketch<A>& sketch);

template<typename A>
static std::ostream& operator<<(std::ostream& os, const HllUnion<A>& hllUnion);

} // namespace datasketches

#include "hll.private.hpp"
//#include "HllSketch.hpp"
//#include "HllUnion.hpp"

#endif // _HLL_HPP_
