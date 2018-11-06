/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLL_H_
#define _HLL_H_

#include <iostream>

namespace datasketches {

// The different types of HLL sketches
enum TgtHllType {
    HLL_4,
    HLL_6,
    HLL_8
};

class HllSketch {
  public:
    static HllSketch* newInstance(const int lgConfigK, const TgtHllType tgtHllType = HLL_4);
    static HllSketch* deserialize(std::istream& is);

    virtual ~HllSketch();

    virtual HllSketch* copy() const = 0;
    virtual HllSketch* copyAs(const TgtHllType tgtHllType) const = 0;

    virtual void reset() = 0;
    
    virtual void serializeCompact(std::ostream& os) const = 0;
    virtual void serializeUpdatable(std::ostream& os) const = 0;

    virtual std::ostream& to_string(std::ostream& os,
                                    const bool summary = true,
                                    const bool detail = false,
                                    const bool auxDetail = false,
                                    const bool all = false) const = 0;

    virtual void update(const std::string datum) = 0;
    virtual void update(const uint64_t datum) = 0;
    virtual void update(const uint32_t datum) = 0;
    virtual void update(const uint16_t datum) = 0;
    virtual void update(const uint8_t datum) = 0;
    virtual void update(const int64_t datum) = 0;
    virtual void update(const int32_t datum) = 0;
    virtual void update(const int16_t datum) = 0;
    virtual void update(const int8_t datum) = 0;
    virtual void update(const double datum) = 0;
    virtual void update(const float datum) = 0;
    virtual void update(const void* data, const size_t lengthBytes) = 0;

    virtual double getEstimate() const = 0;
    virtual double getCompositeEstimate() const = 0;
    virtual double getLowerBound(int numStdDev) const = 0;
    virtual double getUpperBound(int numStdDev) const = 0;

    virtual int getLgConfigK() const = 0;
    virtual TgtHllType getTgtHllType() const = 0;

    virtual bool isCompact() const = 0;
    virtual bool isEmpty() const = 0;

    virtual int getUpdatableSerializationBytes() const = 0;
    virtual int getCompactSerializationBytes() const = 0;

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
    static int getMaxUpdatableSerializationBytes(const int lgK, TgtHllType tgtHllType);
    static double getRelErr(const bool upperBound, const bool unioned,
                            const int lgConfigK, const int numStdDev);

};

class HllUnion {
  public:
    static HllUnion* newInstance(const int lgMaxK);
    static HllUnion* deserialize(std::istream& is);

    virtual ~HllUnion();

    virtual double getEstimate() const = 0;
    virtual double getCompositeEstimate() const = 0;
    virtual double getLowerBound(const int numStdDev) const = 0;
    virtual double getUpperBound(const int numStdDev) const = 0;

    virtual int getCompactSerializationBytes() const = 0;
    virtual int getUpdatableSerializationBytes() const = 0;
    virtual int getLgConfigK() const = 0;

    virtual TgtHllType getTgtHllType() const = 0;
    virtual bool isCompact() const = 0;
    virtual bool isEmpty() const = 0;

    virtual void reset() = 0;

    virtual HllSketch* getResult() const = 0;
    virtual HllSketch* getResult(TgtHllType tgtHllType) const = 0;

    virtual void serializeCompact(std::ostream& os) const = 0;
    virtual void serializeUpdatable(std::ostream& os) const = 0;

    virtual std::ostream& to_string(std::ostream& os,
                                    const bool summary = true,
                                    const bool detail = false,
                                    const bool auxDetail = false,
                                    const bool all = false) const = 0;

    virtual void update(const HllSketch& sketch) = 0;
    virtual void update(const HllSketch* sketch) = 0;
    virtual void update(const std::string datum) = 0;
    virtual void update(const uint64_t datum) = 0;
    virtual void update(const uint32_t datum) = 0;
    virtual void update(const uint16_t datum) = 0;
    virtual void update(const uint8_t datum) = 0;
    virtual void update(const int64_t datum) = 0;
    virtual void update(const int32_t datum) = 0;
    virtual void update(const int16_t datum) = 0;
    virtual void update(const int8_t datum) = 0;
    virtual void update(const double datum) = 0;
    virtual void update(const float datum) = 0;
    virtual void update(const void* data, const size_t lengthBytes) = 0;

    static int getMaxSerializationBytes(const int lgK);
    static double getRelErr(const bool upperBound, const bool unioned,
                            const int lgConfigK, const int numStdDev);

};

std::ostream& operator<<(std::ostream& os, HllSketch& sketch);

} // namespace datasketches

#endif // _HLL_H_