/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLL_HPP_
#define _HLL_HPP_

#include <memory>
#include <iostream>

namespace datasketches {

// The different types of HLL sketches
enum TgtHllType {
    HLL_4,
    HLL_6,
    HLL_8
};

class HllSketch;
class HllUnion;

typedef std::unique_ptr<HllSketch> hll_sketch;
typedef std::unique_ptr<HllUnion> hll_union;

class HllSketch {
  public:
    static hll_sketch newInstance(int lgConfigK, TgtHllType tgtHllType = HLL_4);
    static hll_sketch deserialize(std::istream& is);
    static hll_sketch deserialize(const void* bytes, size_t len);

    virtual ~HllSketch();

    hll_sketch copy() const;
    hll_sketch copyAs(TgtHllType tgtHllType) const;

    virtual void reset() = 0;
    
    virtual std::pair<std::unique_ptr<uint8_t[]>, const size_t> serializeCompact() const = 0;
    virtual std::pair<std::unique_ptr<uint8_t[]>, const size_t> serializeUpdatable() const = 0;
    virtual void serializeCompact(std::ostream& os) const = 0;
    virtual void serializeUpdatable(std::ostream& os) const = 0;
    
    virtual std::ostream& to_string(std::ostream& os,
                                    bool summary = true,
                                    bool detail = false,
                                    bool auxDetail = false,
                                    bool all = false) const = 0;
    virtual std::string to_string(bool summary = true,
                                  bool detail = false,
                                  bool auxDetail = false,
                                  bool all = false) const = 0;                                    

    virtual void update(const std::string& datum) = 0;
    virtual void update(uint64_t datum) = 0;
    virtual void update(uint32_t datum) = 0;
    virtual void update(uint16_t datum) = 0;
    virtual void update(uint8_t datum) = 0;
    virtual void update(int64_t datum) = 0;
    virtual void update(int32_t datum) = 0;
    virtual void update(int16_t datum) = 0;
    virtual void update(int8_t datum) = 0;
    virtual void update(double datum) = 0;
    virtual void update(float datum) = 0;
    virtual void update(const void* data, size_t lengthBytes) = 0;

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
    static int getMaxUpdatableSerializationBytes(int lgK, TgtHllType tgtHllType);
    static double getRelErr(bool upperBound, bool unioned,
                            int lgConfigK, int numStdDev);

};

class HllUnion {
  public:
    static hll_union newInstance(int lgMaxK);
    static hll_union deserialize(std::istream& is);
    static hll_union deserialize(const void* bytes, size_t len);

    virtual ~HllUnion();

    virtual double getEstimate() const = 0;
    virtual double getCompositeEstimate() const = 0;
    virtual double getLowerBound(int numStdDev) const = 0;
    virtual double getUpperBound(int numStdDev) const = 0;

    virtual int getCompactSerializationBytes() const = 0;
    virtual int getUpdatableSerializationBytes() const = 0;
    virtual int getLgConfigK() const = 0;

    virtual TgtHllType getTgtHllType() const = 0;
    virtual bool isCompact() const = 0;
    virtual bool isEmpty() const = 0;

    virtual void reset() = 0;

    virtual hll_sketch getResult(TgtHllType tgtHllType = HLL_4) const = 0;

    virtual std::pair<std::unique_ptr<uint8_t[]>, const size_t> serializeCompact() const = 0;
    virtual std::pair<std::unique_ptr<uint8_t[]>, const size_t> serializeUpdatable() const = 0;
    virtual void serializeCompact(std::ostream& os) const = 0;
    virtual void serializeUpdatable(std::ostream& os) const = 0;

    virtual std::ostream& to_string(std::ostream& os,
                                    bool summary = true,
                                    bool detail = false,
                                    bool auxDetail = false,
                                    bool all = false) const = 0;
    virtual std::string to_string(bool summary = true,
                                  bool detail = false,
                                  bool auxDetail = false,
                                  bool all = false) const = 0;                                    

    virtual void update(const HllSketch& sketch) = 0;
    virtual void update(const std::string& datum) = 0;
    virtual void update(uint64_t datum) = 0;
    virtual void update(uint32_t datum) = 0;
    virtual void update(uint16_t datum) = 0;
    virtual void update(uint8_t datum) = 0;
    virtual void update(int64_t datum) = 0;
    virtual void update(int32_t datum) = 0;
    virtual void update(int16_t datum) = 0;
    virtual void update(int8_t datum) = 0;
    virtual void update(double datum) = 0;
    virtual void update(float datum) = 0;
    virtual void update(const void* data, size_t lengthBytes) = 0;

    static int getMaxSerializationBytes(int lgK);
    static double getRelErr(bool upperBound, bool unioned,
                            int lgConfigK, int numStdDev);
};

std::ostream& operator<<(std::ostream& os, const HllSketch& sketch);
std::ostream& operator<<(std::ostream& os, hll_sketch& sketch);
std::ostream& operator<<(std::ostream& os, const HllUnion& hllUnion);
std::ostream& operator<<(std::ostream& os, hll_union& hllUnion);

} // namespace datasketches

#endif // _HLL_HPP_
