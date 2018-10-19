/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#pragma once

#include "IntArrayPairIterator.hpp"

#include <memory>
#include <stdexcept>

namespace datasketches {

class AuxHashMap {
  public:
    explicit AuxHashMap(int lgAuxArrInts, int lgConfigK);
    explicit AuxHashMap(AuxHashMap& that);
    static AuxHashMap* deserialize(std::istream& is, const int lgConfigK,
                                   const int auxCount, const int lgAuxArrInts,
                                   const bool srcCompact);
    virtual ~AuxHashMap();

    AuxHashMap* copy();
    int getUpdatableSizeBytes();
    int getCompactSizeBytes();

    int getAuxCount();
    int* getAuxIntArr();
    int getLgAuxArrInts();
    std::unique_ptr<PairIterator> getIterator();

    void mustAdd(const int slotNo, const int value);
    int mustFindValueFor(const int slotNo);
    void mustReplace(const int slotNo, const int value);

  private:
    // static so it can be used when resizing
    static int find(const int* auxArr, const int lgAuxArrInts, const int lgConfigK, const int slotNo);

    void checkGrow();
    void growAuxSpace();

    const int lgConfigK;
    int lgAuxArrInts;
    int auxCount;
    int* auxIntArr;
};

}
