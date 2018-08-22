/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "CouponList.hpp"

namespace datasketches {

class CouponHashSet : public CouponList {
  protected:
    explicit CouponHashSet(const int lgConfigK, const TgtHllType tgtHllType);
    explicit CouponHashSet(const CouponHashSet& that);
    explicit CouponHashSet(const CouponHashSet& that, const TgtHllType tgtHllType);

    virtual ~CouponHashSet();

    virtual CouponHashSet* copy();
    virtual CouponHashSet* copyAs(const TgtHllType tgtHllType);

    virtual HllSketchImpl* couponUpdate(int coupon);

    virtual int getMemDataStart();
    virtual int getPreInts();

    friend class CouponList; // so it can access fields declared in CouponList

  private:
    bool checkGrowOrPromote();
    void growHashSet(const int tgtLgCoupArrSize);
};

}
