/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _COUPONLIST_HPP_
#define _COUPONLIST_HPP_

#include "HllSketchImpl.hpp"

namespace datasketches {

class CouponList : public HllSketchImpl {
  public:
    explicit CouponList(int lgConfigK, TgtHllType tgtHllType, CurMode curMode);
    explicit CouponList(const CouponList& that);
    explicit CouponList(const CouponList& that, TgtHllType tgtHllType);

    static CouponList* newList(const void* bytes, size_t len);
    static CouponList* newList(std::istream& is);
    virtual std::pair<std::unique_ptr<uint8_t>, const size_t> serialize(bool compact) const;
    virtual void serialize(std::ostream& os, bool compact) const;

    virtual ~CouponList();

    virtual CouponList* copy() const;
    virtual CouponList* copyAs(TgtHllType tgtHllType) const;

    virtual HllSketchImpl* couponUpdate(int coupon);

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getUpperBound(int numStdDev) const;
    virtual double getLowerBound(int numStdDev) const;

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
    virtual int* getCouponIntArr() const;

    virtual CouponList* reset();

    int lgCouponArrInts;
    int couponCount;
    bool oooFlag;
    int* couponIntArr;
};

}

#endif /* _COUPONLIST_HPP_ */