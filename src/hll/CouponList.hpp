/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "AbstractCoupons.hpp"

namespace datasketches {

class CouponList : public AbstractCoupons {
  public:
    explicit CouponList(const int lgConfigK, const TgtHllType tgtHllType, const CurMode curMode);
    explicit CouponList(const CouponList& that);
    explicit CouponList(const CouponList& that, const TgtHllType tgtHllType);

    virtual ~CouponList();

    virtual CouponList* copy();
    virtual CouponList* copyAs(const TgtHllType tgtHllType);

    virtual HllSketchImpl* couponUpdate(int coupon);

    HllSketchImpl* promoteHeapListToSet(CouponList& list);
    HllSketchImpl* promoteHeapListOrSetToHll(CouponList& src);


  protected:
    virtual int getCouponCount();
    virtual int getCompactSerializationBytes();
    virtual std::unique_ptr<PairIterator> getIterator();
    virtual int getMemDataStart();
    virtual int getPreInts();
    virtual bool isCompact();
    virtual bool isOutOfOrderFlag();
    virtual void putOutOfOrderFlag(bool oooFlag);

    virtual int getLgCouponArrInts();
    virtual int* getCouponIntArr();

    virtual CouponList* reset();

    int lgCouponArrInts;
    int couponCount;
    bool oooFlag;
    int* couponIntArr;
};

}
