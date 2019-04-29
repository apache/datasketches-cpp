/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _COUPONHASHSET_HPP_
#define _COUPONHASHSET_HPP_

#include "CouponList.hpp"

namespace datasketches {

class CouponHashSet : public CouponList {
  public:
    static CouponHashSet* newSet(const void* bytes, size_t len);
    static CouponHashSet* newSet(std::istream& is);

    virtual ~CouponHashSet();

  protected:
    explicit CouponHashSet(int lgConfigK, TgtHllType tgtHllType);
    explicit CouponHashSet(const CouponHashSet& that);
    explicit CouponHashSet(const CouponHashSet& that, TgtHllType tgtHllType);
    
    virtual CouponHashSet* copy() const;
    virtual CouponHashSet* copyAs(TgtHllType tgtHllType) const;

    virtual HllSketchImpl* couponUpdate(int coupon);

    virtual int getMemDataStart() const;
    virtual int getPreInts() const;

    //friend class CouponList; // so it can access fields declared in CouponList
    friend class HllSketchImplFactory;

  private:
    bool checkGrowOrPromote();
    void growHashSet(int srcLgCoupArrSize, int tgtLgCoupArrSize);
};

}

//#include "CouponHashSet-internal.hpp"

#endif /* _COUPONHASHSET_HPP_ */