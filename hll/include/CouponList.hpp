/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _COUPONLIST_HPP_
#define _COUPONLIST_HPP_

#include "HllSketchImpl.hpp"
#include "PairIterator.hpp"

namespace datasketches {

template<typename A>
class HllSketchImplFactory;

template<typename A = std::allocator<char>>
class CouponList : public HllSketchImpl<A> {
  public:
    explicit CouponList(int lgConfigK, TgtHllType tgtHllType, CurMode curMode);
    explicit CouponList(const CouponList& that);
    explicit CouponList(const CouponList& that, TgtHllType tgtHllType);

    static CouponList* newList(const void* bytes, size_t len);
    static CouponList* newList(std::istream& is);
    virtual std::pair<std::unique_ptr<uint8_t, std::function<void(uint8_t*)>>, const size_t> serialize(bool compact, unsigned header_size_bytes) const;
    virtual void serialize(std::ostream& os, bool compact) const;

    virtual ~CouponList();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual CouponList* copy() const;
    virtual CouponList* copyAs(TgtHllType tgtHllType) const;

    virtual HllSketchImpl<A>* couponUpdate(int coupon);

    virtual double getEstimate() const;
    virtual double getCompositeEstimate() const;
    virtual double getUpperBound(int numStdDev) const;
    virtual double getLowerBound(int numStdDev) const;

    virtual bool isEmpty() const;
    virtual int getCouponCount() const;
    virtual PairIterator_with_deleter<A> getIterator() const;

  protected:
    typedef typename std::allocator_traits<A>::template rebind_alloc<CouponList<A>> clAlloc;

    HllSketchImpl<A>* promoteHeapListToSet(CouponList& list);
    HllSketchImpl<A>* promoteHeapListOrSetToHll(CouponList& src);

    virtual int getUpdatableSerializationBytes() const;
    virtual int getCompactSerializationBytes() const;
    virtual int getMemDataStart() const;
    virtual int getPreInts() const;
    virtual bool isCompact() const;
    virtual bool isOutOfOrderFlag() const;
    virtual void putOutOfOrderFlag(bool oooFlag);

    virtual int getLgCouponArrInts() const;
    virtual int* getCouponIntArr() const;

    int lgCouponArrInts;
    int couponCount;
    bool oooFlag;
    int* couponIntArr;

    friend class HllSketchImplFactory<A>;
};

}

#endif /* _COUPONLIST_HPP_ */
