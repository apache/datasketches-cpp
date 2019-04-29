/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _AUXHASHMAP_INTERNAL_HPP_
#define _AUXHASHMAP_INTERNAL_HPP_

#include "HllUtil.hpp"
#include "AuxHashMap.hpp"
#include "IntArrayPairIterator.hpp"

#include <cstring>
#include <sstream>
#include <memory>

namespace datasketches {

AuxHashMap::AuxHashMap(int lgAuxArrInts,int lgConfigK)
  : lgConfigK(lgConfigK),
    lgAuxArrInts(lgAuxArrInts),
    auxCount(0) {
  const int numItems = 1 << lgAuxArrInts;
  auxIntArr = new int[numItems];
  std::fill(auxIntArr, auxIntArr + numItems, 0);
}

AuxHashMap::AuxHashMap(AuxHashMap& that)
  : lgConfigK(that.lgConfigK),
    lgAuxArrInts(that.lgAuxArrInts),
    auxCount(that.auxCount) {
  const int numItems = 1 << lgAuxArrInts;
  auxIntArr = new int[numItems];
  std::copy(that.auxIntArr, that.auxIntArr + numItems, auxIntArr);
}

AuxHashMap* AuxHashMap::deserialize(const void* bytes, size_t len,
                                    int lgConfigK,
                                    int auxCount, int lgAuxArrInts,
                                    bool srcCompact) {
  int lgArrInts = lgAuxArrInts;
  if (srcCompact) { // early compact versions didn't use LgArr byte field so ignore input
    lgArrInts = HllUtil<>::computeLgArrInts(HLL, auxCount, lgConfigK);
  } else { // updatable
    lgArrInts = lgAuxArrInts;
  }

  AuxHashMap* auxHashMap = new AuxHashMap(lgArrInts, lgConfigK);
  int configKmask = (1 << lgConfigK) - 1;

  const int* auxPtr = static_cast<const int*>(bytes);
  if (srcCompact) {
    if (len < auxCount * sizeof(int)) {
      throw std::invalid_argument("Input array too small to hold AuxHashMap image");
    }
    for (int i = 0; i < auxCount; ++i) {
      int pair = auxPtr[i];
      int slotNo = HllUtil<>::getLow26(pair) & configKmask;
      int value = HllUtil<>::getValue(pair);
      auxHashMap->mustAdd(slotNo, value);
    }
  } else { // updatable
    int itemsToRead = 1 << lgAuxArrInts;
    if (len < itemsToRead * sizeof(int)) {
      throw std::invalid_argument("Input array too small to hold AuxHashMap image");
    }
    for (int i = 0; i < itemsToRead; ++i) {
      int pair = auxPtr[i];
      if (pair == HllUtil<>::EMPTY) { continue; }
      int slotNo = HllUtil<>::getLow26(pair) & configKmask;
      int value = HllUtil<>::getValue(pair);
      auxHashMap->mustAdd(slotNo, value);
    }
  }

  if (auxHashMap->getAuxCount() != auxCount) {
    throw std::invalid_argument("Deserialized AuxHashMap has wrong number of entries");
  }

  return auxHashMap;                                    
}


AuxHashMap* AuxHashMap::deserialize(std::istream& is, const int lgConfigK,
                                    const int auxCount, const int lgAuxArrInts,
                                    const bool srcCompact) {
  int lgArrInts = lgAuxArrInts;
  if (srcCompact) { // early compact versions didn't use LgArr byte field so ignore input
    lgArrInts = HllUtil<>::computeLgArrInts(HLL, auxCount, lgConfigK);
  } else { // updatable
    lgArrInts = lgAuxArrInts;
  }
  
  AuxHashMap* auxHashMap = new AuxHashMap(lgArrInts, lgConfigK);
  int configKmask = (1 << lgConfigK) - 1;

  if (srcCompact) {
    int pair;
    for (int i = 0; i < auxCount; ++i) {
      is.read((char*)&pair, sizeof(pair));
      int slotNo = HllUtil<>::getLow26(pair) & configKmask;
      int value = HllUtil<>::getValue(pair);
      auxHashMap->mustAdd(slotNo, value);
    }
  } else { // updatable
    int itemsToRead = 1 << lgAuxArrInts;
    int pair;
    for (int i = 0; i < itemsToRead; ++i) {
      is.read((char*)&pair, sizeof(pair));
      if (pair == HllUtil<>::EMPTY) { continue; }
      int slotNo = HllUtil<>::getLow26(pair) & configKmask;
      int value = HllUtil<>::getValue(pair);
      auxHashMap->mustAdd(slotNo, value);
    }
  }

  if (auxHashMap->getAuxCount() != auxCount) {
    throw std::invalid_argument("Deserialized AuxHashMap has wrong number of entries");
  }

  return auxHashMap;
}

AuxHashMap::~AuxHashMap() {
  // should be no way to have an object without a valid array
  delete auxIntArr;
}

AuxHashMap* AuxHashMap::copy() {
  return new AuxHashMap(*this);
}

int AuxHashMap::getAuxCount() {
  return auxCount;
}

int* AuxHashMap::getAuxIntArr() {
  return auxIntArr;
}

int AuxHashMap::getLgAuxArrInts() {
  return lgAuxArrInts;
}

int AuxHashMap::getCompactSizeBytes() {
  return auxCount << 2;
}

int AuxHashMap::getUpdatableSizeBytes() {
  return 4 << lgAuxArrInts;
}

std::unique_ptr<PairIterator> AuxHashMap::getIterator() {
  PairIterator* itr = new IntArrayPairIterator(auxIntArr, 1 << lgAuxArrInts, lgConfigK);
  return std::unique_ptr<PairIterator>(itr);
}

void AuxHashMap::mustAdd(const int slotNo, const int value) {
  const int index = find(auxIntArr, lgAuxArrInts, lgConfigK, slotNo);
  const int entry_pair = HllUtil<>::pair(slotNo, value);
  if (index >= 0) {
    throw std::invalid_argument("Found a slotNo that should not be there: SlotNo: "
                                + std::to_string(slotNo) + ", Value: " + std::to_string(value));
  }

  // found empty entry
  auxIntArr[~index] = entry_pair;
  ++auxCount;
  checkGrow();
}

int AuxHashMap::mustFindValueFor(const int slotNo) {
  const int index = find(auxIntArr, lgAuxArrInts, lgConfigK, slotNo);
  if (index >= 0) {
    return HllUtil<>::getValue(auxIntArr[index]);
  }

  throw std::invalid_argument("slotNo not found: " + std::to_string(slotNo));
}

void AuxHashMap::mustReplace(const int slotNo, const int value) {
  const int idx = find(auxIntArr, lgAuxArrInts, lgConfigK, slotNo);
  if (idx >= 0) {
    auxIntArr[idx] = HllUtil<>::pair(slotNo, value);
    return;
  }

  throw std::invalid_argument("Pair not found: SlotNo: " + std::to_string(slotNo)
                              + ", Value: " + std::to_string(value));
}

void AuxHashMap::checkGrow() {
  if ((HllUtil<>::RESIZE_DENOM * auxCount) > (HllUtil<>::RESIZE_NUMER * (1 << lgAuxArrInts))) {
    growAuxSpace();
  }
}

void AuxHashMap::growAuxSpace() {
  int* oldArray = auxIntArr;
  const int oldArrLen = 1 << lgAuxArrInts;
  const int configKmask = (1 << lgConfigK) - 1;
  const int newArrLen = 1 << ++lgAuxArrInts;
  auxIntArr = new int[newArrLen];
  std::fill(auxIntArr, auxIntArr + newArrLen, 0);
  for (int i = 0; i < oldArrLen; ++i) {
    const int fetched = oldArray[i];
    if (fetched != HllUtil<>::EMPTY) {
      // find empty in new array
      const int idx = find(auxIntArr, lgAuxArrInts, lgConfigK, fetched & configKmask);
      auxIntArr[~idx] = fetched;
    }
  }

  delete oldArray;
}

//Searches the Aux arr hash table for an empty or a matching slotNo depending on the context.
//If entire entry is empty, returns one's complement of index = found empty.
//If entry contains given slotNo, returns its index = found slotNo.
//Continues searching.
//If the probe comes back to original index, throws an exception.
int AuxHashMap::find(const int* auxArr, const int lgAuxArrInts, const int lgConfigK,
                     const int slotNo) {
  const int auxArrMask = (1 << lgAuxArrInts) - 1;
  const int configKmask = (1 << lgConfigK) - 1;
  int probe = slotNo & auxArrMask;
  const  int loopIndex = probe;
  do {
    const int arrVal = auxArr[probe];
    if (arrVal == HllUtil<>::EMPTY) { //Compares on entire entry
      return ~probe; //empty
    }
    else if (slotNo == (arrVal & configKmask)) { //Compares only on slotNo
      return probe; //found given slotNo, return probe = index into aux array
    }
    const int stride = (slotNo >> lgAuxArrInts) | 1;
    probe = (probe + stride) & auxArrMask;
  } while (probe != loopIndex);
  throw std::runtime_error("Key not found and no empty slots!");
}

}

#endif // _AUXHASHMAP_INTERNAL_HPP_
