/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "HllArray.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

class Hll8Array : public HllArray {
  public:
    explicit Hll8Array(const int lgConfigK);
    explicit Hll8Array(const Hll8Array& that);

    virtual ~Hll8Array();

    virtual Hll8Array* copy() const;

    virtual std::unique_ptr<PairIterator> getIterator() const;

    virtual int getSlot(const int slotNo) const;
    virtual void putSlot(const int slotNo, const int value);

    virtual int getHllByteArrBytes() const;

  protected:
    friend class Hll8Iterator;
};

class Hll8Iterator : public HllPairIterator {
  public:
    Hll8Iterator(const Hll8Array& array, const int lengthPairs);
    virtual int value();

    virtual ~Hll8Iterator();

  private:
    const Hll8Array& hllArray;
};

}
