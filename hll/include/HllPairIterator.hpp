/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLPAIRITERATOR_HPP_
#define _HLLPAIRITERATOR_HPP_

#include "PairIterator.hpp"
#include "HllArray.hpp"

namespace datasketches {
  
class HllPairIterator : public PairIterator {
  public:
    HllPairIterator(const int lengthPairs);
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
    virtual int value() = 0;

    const int lengthPairs;
    int index;
    int val;
};

}

#endif /* _HLLPAIRITERATOR_HPP_ */