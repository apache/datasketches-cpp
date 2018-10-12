/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLSKETCH_H_
#define _HLLSKETCH_H_

#include "hll.hpp"
#include "PairIterator.hpp"
#include "HllUtil.hpp"
#include "HllSketchImpl.hpp"

#include <memory>
#include <iostream>

namespace datasketches {

class HllSketchImpl;

// Contains the non-public API for HllSketch
class HllSketchPvt : public HllSketch {
  public:
    explicit HllSketchPvt(const int lgConfigK, const TgtHllType tgtHllType = HLL_4);
    static HllSketchPvt* deserialize(std::istream& is);

    virtual ~HllSketchPvt();

    // copy constructors
    HllSketchPvt(const HllSketch& that);
    HllSketchPvt(HllSketchImpl* that);

    HllSketch* copy() const;
    HllSketch* copyAs(const TgtHllType tgtHllType) const;

    virtual void reset();

    virtual void update(const std::string datum);
    virtual void update(const uint64_t datum);
    virtual void update(const uint32_t datum);
    virtual void update(const uint16_t datum);
    virtual void update(const uint8_t datum);
    virtual void update(const int64_t datum);
    virtual void update(const int32_t datum);
    virtual void update(const int16_t datum);
    virtual void update(const int8_t datum);
    virtual void update(const double datum);
    virtual void update(const float datum);
    virtual void update(const void* data, const size_t lengthBytes);
    
    virtual void serializeCompact(std::ostream& os) const;
    virtual void serializeUpdatable(std::ostream& os) const;

    virtual std::ostream& to_string(std::ostream& os,
                                    const bool summary = true,
                                    const bool detail = false,
                                    const bool auxDetail = false,
                                    const bool all = false) const;

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getLowerBound(int numStdDev) const;
    virtual double getUpperBound(int numStdDev) const;

    virtual int getLgConfigK() const;
    virtual TgtHllType getTgtHllType() const;
    
    virtual bool isCompact() const;
    virtual bool isEmpty() const;

    virtual int getUpdatableSerializationBytes() const;
    virtual int getCompactSerializationBytes() const;

    virtual std::unique_ptr<PairIterator> getIterator() const;

    virtual void couponUpdate(int coupon);

    virtual std::string typeAsString() const;
    virtual std::string modeAsString() const;

    CurMode getCurrentMode() const;
    int getSerializationVersion() const;
    bool isOutOfOrderFlag() const;
    bool isEstimationMode() const;

    HllSketchImpl* hllSketchImpl;
};

}

#endif // _HLLSKETCH_H_
