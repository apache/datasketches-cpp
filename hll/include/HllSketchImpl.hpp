/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLSKETCHIMPL_H_
#define _HLLSKETCHIMPL_H_

#include "HllUtil.hpp"
#include "HllSketch.hpp"

#include <memory>

namespace datasketches {

class HllSketchImpl {
  public:
    HllSketchImpl(int lgConfigK, TgtHllType tgtHllType, CurMode curMode);
    virtual ~HllSketchImpl();

    virtual void serialize(std::ostream& os, bool compact) const = 0;
    static HllSketchImpl* deserialize(std::istream& os);

    virtual HllSketchImpl* copy() const = 0;
    virtual HllSketchImpl* copyAs(TgtHllType tgtHllType) const = 0;
    virtual HllSketchImpl* reset() = 0;

    virtual HllSketchImpl* couponUpdate(int coupon) = 0;

    virtual CurMode getCurMode() const;

    virtual double getEstimate() const = 0;
    virtual double getCompositeEstimate() const = 0;
    virtual double getUpperBound(int numStdDev) const = 0;
    virtual double getLowerBound(int numStdDev) const = 0;

    virtual std::unique_ptr<PairIterator> getIterator() const = 0;

    int getLgConfigK() const;

    virtual int getMemDataStart() const = 0;

    virtual int getPreInts() const = 0;

    TgtHllType getTgtHllType() const;

    virtual int getUpdatableSerializationBytes() const = 0;
    virtual int getCompactSerializationBytes() const = 0;

    virtual bool isCompact() const = 0;
    virtual bool isEmpty() const = 0;
    virtual bool isOutOfOrderFlag() const = 0;
    virtual void putOutOfOrderFlag(bool oooFlag) = 0;

  protected:
    static TgtHllType extractTgtHllType(uint8_t modeByte);
    static CurMode extractCurMode(uint8_t modeByte);
    uint8_t makeFlagsByte(bool compact) const;
    uint8_t makeModeByte() const;

    const int lgConfigK;
    const TgtHllType tgtHllType;
    const CurMode curMode;
};

}

#endif // _HLLSKETCHIMPL_H_