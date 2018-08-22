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
    explicit Hll8Array(Hll8Array& that);

    virtual ~Hll8Array();

    virtual Hll8Array* copy();

    virtual std::unique_ptr<PairIterator> getIterator();

    virtual int getSlot(const int slotNo);
    virtual void putSlot(const int slotNo, const int value);

    virtual int getHllByteArrBytes();

  protected:
    friend class Hll8Iterator;
};

class Hll8Iterator : public HllPairIterator {
  public:
    Hll8Iterator(Hll8Array& array, const int lengthPairs);
    virtual int value();

    virtual ~Hll8Iterator();

  private:
    Hll8Array& hllArray;
};

}
