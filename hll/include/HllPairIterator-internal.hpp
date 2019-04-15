/*
 * Copyright 2018, Yahoo! Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#ifndef _HLLPAIRITERATOR_INTERNAL_HPP_
#define _HLLPAIRITERATOR_INTERNAL_HPP_

#include <sstream>
#include <iomanip>

#include "HllUtil.hpp"
#include "HllPairIterator.hpp"

namespace datasketches {

HllPairIterator::HllPairIterator(const int lengthPairs)
  : lengthPairs(lengthPairs),
    index(-1),
    val(-1)
{ }

std::string HllPairIterator::getHeader() {
  std::ostringstream ss;
  ss << std::setw(10) << "Slot" << std::setw(6) << "Value";
  return ss.str();
}

int HllPairIterator::getIndex() {
  return index;
}

int HllPairIterator::getKey() {
  return index;
}

int HllPairIterator::getSlot() {
  return index;
}

int HllPairIterator::getPair() {
  return HllUtil::pair(index, val);
}

int HllPairIterator::getValue() {
  return val;
}

std::string HllPairIterator::getString() {
  std::ostringstream ss;
  ss << std::setw(10) << getSlot() << std::setw(6) << getValue();
  return ss.str();
}

bool HllPairIterator::nextAll() {
  if (++index < lengthPairs) {
    val = value();
    return true;
  }
  return false;
}

bool HllPairIterator::nextValid() {
  while (++index < lengthPairs) {
    val = value();
    if (val != HllUtil::EMPTY) {
      return true;
    }
  }
  return false;
}

}

#endif // _HLLPAIRITERATOR_INTERNAL_HPP_
