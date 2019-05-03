/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _AUXHASHMAP_HPP_
#define _AUXHASHMAP_HPP_

#include "PairIterator.hpp"

#include <memory>

namespace datasketches {

template<typename A = std::allocator<char>>
class AuxHashMap final {
  public:
    explicit AuxHashMap(int lgAuxArrInts, int lgConfigK);
    explicit AuxHashMap(AuxHashMap<A>& that);
    static AuxHashMap* newAuxHashMap(int lgAuxArrInts, int lgConfigK);
    static AuxHashMap* newAuxHashMap(AuxHashMap<A>& that);

    static AuxHashMap* deserialize(const void* bytes, size_t len,
                                   int lgConfigK,
                                   int auxCount, int lgAuxArrInts,
                                   bool srcCompact);
    static AuxHashMap* deserialize(std::istream& is, int lgConfigK,
                                   int auxCount, int lgAuxArrInts,
                                   bool srcCompact);
    virtual ~AuxHashMap();
    static std::function<void(AuxHashMap<A>*)> make_deleter();
    
    AuxHashMap* copy() const;
    int getUpdatableSizeBytes() const;
    int getCompactSizeBytes() const;

    int getAuxCount() const;
    int* getAuxIntArr();
    int getLgAuxArrInts() const;
    std::unique_ptr<PairIterator<A>> getIterator() const;

    void mustAdd(int slotNo, int value);
    int mustFindValueFor(int slotNo);
    void mustReplace(int slotNo, int value);

  private:
    typedef typename std::allocator_traits<A>::template rebind_alloc<AuxHashMap<A>> ahmAlloc;

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

//#include "AuxHashMap-internal.hpp"

#endif /* _AUXHASHMAP_HPP_ */