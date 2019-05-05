/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _PAIRITERATOR_HPP_
#define _PAIRITERATOR_HPP_

#include <string>

namespace datasketches {

template<typename A = std::allocator<char>>
class PairIterator {
  public:
    virtual std::string getHeader() = 0;

    virtual int getIndex() = 0;
    virtual int getKey() = 0;
    virtual int getPair() = 0;
    virtual int getSlot() = 0;

    virtual std::string getString() = 0;

    virtual int getValue() = 0;
    virtual bool nextAll() = 0;
    virtual bool nextValid() = 0;

    virtual ~PairIterator() {}
};

template<typename A>
using PairIterator_with_deleter = std::unique_ptr<PairIterator<A>, std::function<void(PairIterator<A>*)>>;

}

#endif /* _PAIRITERATOR_HPP_ */