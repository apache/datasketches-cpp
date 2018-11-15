/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "HllArray.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

class Hll6Array : public HllArray {
  public:
    explicit Hll6Array(const int lgConfigK);
    explicit Hll6Array(const Hll6Array& that);

    virtual ~Hll6Array();

    virtual Hll6Array* copy() const;

    virtual std::unique_ptr<PairIterator> getIterator() const;

    virtual int getSlot(const int slotNo) const;
    virtual void putSlot(const int slotNo, const int value);

    virtual int getHllByteArrBytes() const;

  protected:
    friend class Hll6Iterator;
};

class Hll6Iterator : public HllPairIterator {
  public:
    Hll6Iterator(const Hll6Array& array, const int lengthPairs);
    virtual int value();

    virtual ~Hll6Iterator();

  private:
    const Hll6Array& hllArray;
    int bitOffset;
};

}
