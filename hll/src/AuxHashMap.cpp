/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "HllUtil.hpp"
#include "AuxHashMap.hpp"

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

AuxHashMap* AuxHashMap::deserialize(std::istream& is, const int lgConfigK,
                                    const int auxCount, const int lgAuxArrInts,
                                    const bool srcCompact) {
  int lgArrInts = lgAuxArrInts;
  if (srcCompact) { // early compact versions didn't use LgArr byte field so ignore input
    int ceilInts = HllUtil::ceilingPowerOf2(auxCount);
    if ((HllUtil::RESIZE_DENOM * auxCount) > (HllUtil::RESIZE_NUMER * ceilInts)) {
      ceilInts <<= 1;
    }
    int maxVal = (ceilInts > (1 << HllUtil::LG_AUX_ARR_INTS[lgConfigK])
                  ? ceilInts : (1 << HllUtil::LG_AUX_ARR_INTS[lgConfigK]));
    lgArrInts = HllUtil::simpleIntLog2(maxVal);
  }
  
  AuxHashMap* auxHashMap = new AuxHashMap(lgArrInts, lgConfigK);
  int configKmask = (1 << lgConfigK) - 1;

  int itemsToRead = (srcCompact ? auxCount : (1 << lgAuxArrInts));
  for (int i = 0; i < itemsToRead; ++i) {
    int pair;
    is.read((char*)&pair, sizeof(pair));
    int slotNo = HllUtil::getLow26(pair) & configKmask;
    int value = HllUtil::getValue(pair);
    auxHashMap->mustAdd(slotNo, value);
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
  const int entry_pair = HllUtil::pair(slotNo, value);
  if (index >= 0) {
    std::ostringstream oss;
    oss << "Found a slotNo that should not be there: "
        << "SlotNo: " << slotNo
        << ", Value: " << value;
    throw std::invalid_argument(oss.str());
  }

  // found empty entry
  auxIntArr[~index] = entry_pair;
  ++auxCount;
  checkGrow();
}

int AuxHashMap::mustFindValueFor(const int slotNo) {
  const int index = find(auxIntArr, lgAuxArrInts, lgConfigK, slotNo);
  if (index >= 0) {
    return HllUtil::getValue(auxIntArr[index]);
  }

  std::ostringstream oss;
  oss << "slotNo not found: " << slotNo;
  throw std::invalid_argument(oss.str());
}

void AuxHashMap::mustReplace(const int slotNo, const int value) {
  const int idx = find(auxIntArr, lgAuxArrInts, lgConfigK, slotNo);
  if (idx >= 0) {
    auxIntArr[idx] = HllUtil::pair(slotNo, value);
    return;
  }
  std::ostringstream oss;
  oss << "Pair not found: "
      << "SlotNo: " << slotNo
      << ", Value: " << value;
  throw std::invalid_argument(oss.str());
}

void AuxHashMap::checkGrow() {
  if ((HllUtil::RESIZE_DENOM * auxCount) > (HllUtil::RESIZE_NUMER * (1 << lgAuxArrInts))) {
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
    if (fetched != HllUtil::EMPTY) {
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
  //assert lgAuxArrInts < lgConfigK;
  const int auxArrMask = (1 << lgAuxArrInts) - 1;
  const int configKmask = (1 << lgConfigK) - 1;
  int probe = slotNo & auxArrMask;
  const  int loopIndex = probe;
  do {
    const int arrVal = auxArr[probe];
    if (arrVal == HllUtil::EMPTY) { //Compares on entire entry
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
