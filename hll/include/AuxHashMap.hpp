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