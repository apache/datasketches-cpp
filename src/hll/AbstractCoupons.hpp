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

    virtual int getCouponCount() const = 0;

    virtual int getLgCouponArrInts() const = 0;
    virtual int* getCouponIntArr() = 0;

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getUpperBound(const int numStdDev) const;
    virtual double getLowerBound(const int numStdDev) const;

    virtual int getUpdatableSerializationBytes() const;

    virtual bool isEmpty() const;
};

}
