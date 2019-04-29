/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _INTARRAYPAIRITERATOR_HPP_
#define _INTARRAYPAIRITERATOR_HPP_

#include "PairIterator.hpp"

namespace datasketches {

class IntArrayPairIterator : public PairIterator {
  public:
    explicit IntArrayPairIterator(const int* array, int len, int lgConfigK);

    virtual ~IntArrayPairIterator();

    virtual std::string getHeader();

    virtual int getIndex();
    virtual int getKey();
    virtual int getPair();
    virtual int getSlot();

    virtual std::string getString();

    virtual int getValue();
    virtual bool nextAll();
    virtual bool nextValid();

  protected:
    const int* array;
    const int slotMask;
    const int lengthPairs;
    int index;
    int pair;
};

}

//#include "IntArrayPairIterator-internal.hpp"

#endif /* _INTARRAYPAIRITERATOR_HPP_ */