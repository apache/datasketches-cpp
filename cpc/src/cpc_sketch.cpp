/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <iostream>

#include "cpc_sketch.hpp"

namespace datasketches {

void cpc_init(void* (*alloc)(size_t), void (*dealloc)(void*)) {
  fm85InitAD(alloc, dealloc);
}

// optional deallocation of globally allocated compression tables
void cpc_cleanup() {
  fm85Clean();
}

std::ostream& operator<<(std::ostream& os, cpc_sketch const& sketch) {
  os << "### CPC sketch summary:" << std::endl;
  os << "   lgK            : " << sketch.state->lgK << std::endl;
  os << "   seed hash      : " << std::hex << compute_seed_hash(sketch.seed) << std::dec << std::endl;
  os << "   C              : " << sketch.state->numCoupons << std::endl;
  os << "   flavor         : " << determineFlavor(sketch.state->lgK, sketch.state->numCoupons) << std::endl;
  os << "   merged         : " << (sketch.state->mergeFlag ? "true" : "false") << std::endl;
  os << "   compressed     : " << (sketch.state->isCompressed ? "true" : "false") << std::endl;
  os << "   intresting col : " << sketch.state->firstInterestingColumn << std::endl;
  os << "   HIP estimate   : " << sketch.state->hipEstAccum << std::endl;
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
