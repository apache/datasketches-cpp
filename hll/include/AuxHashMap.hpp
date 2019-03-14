/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _AUXHASHMAP_HPP_
#define _AUXHASHMAP_HPP_

#include "IntArrayPairIterator.hpp"

#include <memory>

namespace datasketches {

class AuxHashMap final {
  public:
    explicit AuxHashMap(int lgAuxArrInts, int lgConfigK);
    explicit AuxHashMap(AuxHashMap& that);
    static AuxHashMap* deserialize(const void* bytes, size_t len,
                                   int lgConfigK,
                                   int auxCount, int lgAuxArrInts,
                                   bool srcCompact);
    static AuxHashMap* deserialize(std::istream& is, int lgConfigK,
                                   int auxCount, int lgAuxArrInts,
                                   bool srcCompact);
    virtual ~AuxHashMap();

    AuxHashMap* copy();
    int getUpdatableSizeBytes();
    int getCompactSizeBytes();

    int getAuxCount();
    int* getAuxIntArr();
    int getLgAuxArrInts();
    std::unique_ptr<PairIterator> getIterator();

    void mustAdd(int slotNo, int value);
    int mustFindValueFor(int slotNo);
    void mustReplace(int slotNo, int value);

  private:
    // static so it can be used when resizing
    static int find(const int* auxArr, int lgAuxArrInts, int lgConfigK, int slotNo);

    void checkGrow();
    void growAuxSpace();

    const int lgConfigK;
    int lgAuxArrInts;
    int auxCount;
    int* auxIntArr;
};

}

#endif /* _AUXHASHMAP_HPP_ */