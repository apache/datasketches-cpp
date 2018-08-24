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

    virtual CouponList* copy() const;
    virtual CouponList* copyAs(const TgtHllType tgtHllType) const;

    virtual HllSketchImpl* couponUpdate(int coupon);

    virtual void serialize(std::ostream& os, const bool comapct) const;

  protected:
    HllSketchImpl* promoteHeapListToSet(CouponList& list);
    HllSketchImpl* promoteHeapListOrSetToHll(CouponList& src);

    virtual int getCouponCount() const;
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
