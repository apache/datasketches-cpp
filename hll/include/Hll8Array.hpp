/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLL8ARRAY_HPP_
#define _HLL8ARRAY_HPP_

#include "HllArray.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

class Hll8Array final : public HllArray {
  public:
    explicit Hll8Array(int lgConfigK);
    explicit Hll8Array(const Hll8Array& that);

    virtual ~Hll8Array();

    virtual Hll8Array* copy() const;

    virtual std::unique_ptr<PairIterator> getIterator() const;

    virtual int getSlot(int slotNo) const final;
    virtual void putSlot(int slotNo, int value) final;

    virtual HllSketchImpl* couponUpdate(int coupon) final;

    virtual int getHllByteArrBytes() const;

  protected:
    friend class Hll8Iterator;
};

class Hll8Iterator : public HllPairIterator {
  public:
    Hll8Iterator(const Hll8Array& array, int lengthPairs);
    virtual int value();

    virtual ~Hll8Iterator();

  private:
    const Hll8Array& hllArray;
};

}

//#include "Hll8Array-internal.hpp"

#endif /* _HLL8ARRAY_HPP_ */