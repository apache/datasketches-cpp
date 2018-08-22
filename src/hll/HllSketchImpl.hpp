/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "HllSketch.hpp"

#include <memory>

namespace datasketches {

class HllSketch;

class HllSketchImpl {
  public:
    HllSketchImpl(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode);
    virtual ~HllSketchImpl();

    virtual HllSketchImpl* copy() = 0;
    virtual HllSketchImpl* copyAs(TgtHllType tgtHllType) = 0;
    virtual HllSketchImpl* reset() = 0;

    virtual HllSketchImpl* couponUpdate(int coupon) = 0;

    CurMode getCurMode();

    virtual double getEstimate() = 0;
    virtual double getCompositeEstimate() = 0;
    virtual double getUpperBound(int numStdDev) = 0;
    virtual double getLowerBound(int numStdDev) = 0;

    virtual std::unique_ptr<PairIterator> getIterator() = 0;

    int getLgConfigK();

    virtual int getMemDataStart() = 0;

    virtual int getPreInts() = 0;

    TgtHllType getTgtHllType();

    virtual int getUpdatableSerializationBytes() = 0;
    virtual int getCompactSerializationBytes() = 0;

    virtual bool isCompact() = 0;
    virtual bool isEmpty() = 0;
    virtual bool isOutOfOrderFlag() = 0;
    virtual void putOutOfOrderFlag(bool oooFlag) = 0;

    //virtual byte[] toCompactByteArray();

    //virtual byte[] toUpdatableByteArray();

  protected:
    const int lgConfigK;
    const TgtHllType tgtHllType;
    const CurMode curMode;
};

}
