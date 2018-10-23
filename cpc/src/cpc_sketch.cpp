/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <iostream>

#include "cpc_sketch.hpp"

namespace datasketches {

std::ostream& operator<<(std::ostream& os, cpc_sketch const& sketch) {
  os << "### CPC sketch summary:" << std::endl;
  os << "   lgK            : " << sketch.state->lgK << std::endl;
  os << "   C              : " << sketch.state->numCoupons << std::endl;
  os << "   flavor         : " << determineFlavor(sketch.state->lgK, sketch.state->numCoupons) << std::endl;
  os << "   merged         : " << (sketch.state->mergeFlag ? "true" : "false") << std::endl;
  os << "   compressed     : " << (sketch.state->isCompressed ? "true" : "false") << std::endl;
  os << "   intresting col : " << sketch.state->firstInterestingColumn << std::endl;
  os << "   HIP estimate   : " << sketch.state->hipEstAccum << std::endl;
  //os << "   HIP error      : " << sketch.state->hipErrAccum << std::endl;
  os << "   kxp            : " << sketch.state->kxp << std::endl;
  if (sketch.state->isCompressed) {
    os << "   num CSV        : " << sketch.state->numCompressedSurprisingValues << std::endl;
    os << "   CSV length     : " << sketch.state->csvLength << std::endl;
    os << "   CW length      : " << sketch.state->cwLength << std::endl;
  } else {
    os << "   offset         : " << sketch.state->windowOffset << std::endl;
    os << "   table          : " << (sketch.state->surprisingValueTable == nullptr ? "not " : "") <<  "allocated" << std::endl;
    if (sketch.state->surprisingValueTable != nullptr) {
      os << "   num SV         : " << sketch.state->surprisingValueTable->numItems << std::endl;
    }
    os << "   window         : " << (sketch.state->slidingWindow == nullptr ? "not " : "") <<  "allocated" << std::endl;
  }
  os << "### End sketch summary" << std::endl;
  return os;
}

} /* namespace datasketches */
