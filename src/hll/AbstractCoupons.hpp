/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "HllSketchImpl.hpp"

namespace datasketches {

class AbstractCoupons : public HllSketchImpl {
  public:
    AbstractCoupons(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode);
    virtual ~AbstractCoupons();

    virtual int getCouponCount() = 0;

    virtual int getLgCouponArrInts() = 0;
    virtual int* getCouponIntArr() = 0;

    virtual double getEstimate();
    virtual double getCompositeEstimate();
    virtual double getUpperBound(const int numStdDev);
    virtual double getLowerBound(const int numStdDev);

    virtual int getUpdatableSerializationBytes();
    //virtual int getCompactSerializationBytes();

    //virtual int getPreInts();
    //virtual int getMemDataStart();

    virtual bool isEmpty();

    //virtual bool isCompact();

    //virtual HllSketchImpl* copy();

    //virtual HllSketchImpl* copyAs(TgtHllType tgtHllType);

    //virtual bool isOutOfOrderFlag();
    //virtual void putOutOfOrderFlag(bool oooFlag);

    //virtual HllSketchImpl* reset() = 0;

    //char[] toCompactByteArray();

    //char[] toUpdateableByteArray();
};

}
