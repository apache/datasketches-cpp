/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLL6ARRAY_HPP_
#define _HLL6ARRAY_HPP_

#include "HllArray.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

class Hll6Array final : public HllArray {
  public:
    explicit Hll6Array(int lgConfigK);
    explicit Hll6Array(const Hll6Array& that);

    virtual ~Hll6Array();

    virtual Hll6Array* copy() const;

    virtual std::unique_ptr<PairIterator> getIterator() const;

    virtual int getSlot(int slotNo) const final;
    virtual void putSlot(int slotNo, int value) final;

    virtual HllSketchImpl* couponUpdate(int coupon) final;

    virtual int getHllByteArrBytes() const;

  protected:
    friend class Hll6Iterator;
};

class Hll6Iterator : public HllPairIterator {
  public:
    Hll6Iterator(const Hll6Array& array, int lengthPairs);
    virtual int value();

    virtual ~Hll6Iterator();

  private:
    const Hll6Array& hllArray;
    int bitOffset;
};

}

#include "Hll6Array-internal.hpp"

#endif /* _HLL6ARRAY_HPP_ */
