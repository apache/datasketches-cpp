/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _COUPONHASHSET_HPP_
#define _COUPONHASHSET_HPP_

#include "CouponList.hpp"

namespace datasketches {

template<typename A = std::allocator<char>>
class CouponHashSet : public CouponList<A> {
  public:
    static CouponHashSet* newSet(const void* bytes, size_t len);
    static CouponHashSet* newSet(std::istream& is);
    explicit CouponHashSet(int lgConfigK, TgtHllType tgtHllType);
    explicit CouponHashSet(const CouponHashSet& that, TgtHllType tgtHllType);
    explicit CouponHashSet(const CouponHashSet& that);

    virtual ~CouponHashSet();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

  protected:
    
    virtual CouponHashSet* copy() const;
    virtual CouponHashSet* copyAs(TgtHllType tgtHllType) const;

    virtual HllSketchImpl<A>* couponUpdate(int coupon);

    virtual int getMemDataStart() const;
    virtual int getPreInts() const;

    friend class HllSketchImplFactory<A>;

  private:
    typedef typename std::allocator_traits<A>::template rebind_alloc<CouponHashSet<A>> chsAlloc;
    bool checkGrowOrPromote();
    void growHashSet(int srcLgCoupArrSize, int tgtLgCoupArrSize);
};

}

//#include "CouponHashSet-internal.hpp"

#endif /* _COUPONHASHSET_HPP_ */