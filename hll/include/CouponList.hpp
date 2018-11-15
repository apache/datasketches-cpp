/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "HllSketchImpl.hpp"

namespace datasketches {

class CouponList : public HllSketchImpl {
  public:
    explicit CouponList(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode);
    explicit CouponList(const CouponList& that);
    explicit CouponList(const CouponList& that, const TgtHllType tgtHllType);

    static CouponList* newList(std::istream& is);
    virtual void serialize(std::ostream& os, const bool compact) const;

    virtual ~CouponList();

    virtual CouponList* copy() const;
    virtual CouponList* copyAs(const TgtHllType tgtHllType) const;

    virtual HllSketchImpl* couponUpdate(int coupon);

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getUpperBound(const int numStdDev) const;
    virtual double getLowerBound(const int numStdDev) const;

    virtual bool isEmpty() const;
    virtual int getCouponCount() const;

  protected:
    HllSketchImpl* promoteHeapListToSet(CouponList& list);
    HllSketchImpl* promoteHeapListOrSetToHll(CouponList& src);

    virtual int getUpdatableSerializationBytes() const;
    virtual int getCompactSerializationBytes() const;
    virtual std::unique_ptr<PairIterator> getIterator() const;
    virtual int getMemDataStart() const;
    virtual int getPreInts() const;
    virtual bool isCompact() const;
    virtual bool isOutOfOrderFlag() const;
    virtual void putOutOfOrderFlag(bool oooFlag);

    virtual int getLgCouponArrInts() const;
    virtual int* getCouponIntArr();

    virtual CouponList* reset();

    int lgCouponArrInts;
    int couponCount;
    bool oooFlag;
    int* couponIntArr;
};

}
