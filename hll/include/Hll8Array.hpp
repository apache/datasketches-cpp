/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLL8ARRAY_HPP_
#define _HLL8ARRAY_HPP_

#include "HllArray.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

template<typename A>
class Hll8Iterator;

template<typename A>
class Hll8Array final : public HllArray<A> {
  public:
    explicit Hll8Array(int lgConfigK, bool startFullSize);
    explicit Hll8Array(const Hll8Array& that);

    virtual ~Hll8Array();
    virtual std::function<void(HllSketchImpl<A>*)> get_deleter() const;

    virtual Hll8Array<A>* copy() const;

    virtual PairIterator_with_deleter<A> getIterator() const;

    virtual int getSlot(int slotNo) const final;
    virtual void putSlot(int slotNo, int value) final;

    virtual HllSketchImpl<A>* couponUpdate(int coupon) final;

    virtual int getHllByteArrBytes() const;

  protected:
    friend class Hll8Iterator<A>;
};

template<typename A>
class Hll8Iterator : public HllPairIterator<A> {
  public:
    Hll8Iterator(const Hll8Array<A>& array, int lengthPairs);
    virtual int value();

    virtual ~Hll8Iterator();

  private:
    const Hll8Array<A>& hllArray;
};

}

//#include "Hll8Array-internal.hpp"

#endif /* _HLL8ARRAY_HPP_ */