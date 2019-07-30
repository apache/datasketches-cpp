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

#ifndef _HLLPAIRITERATOR_HPP_
#define _HLLPAIRITERATOR_HPP_

#include "PairIterator.hpp"
#include "HllArray.hpp"

namespace datasketches {

template<typename A>
class HllPairIterator : public PairIterator<A> {
  public:
    HllPairIterator(const int lengthPairs);
    virtual ~HllPairIterator() = default;
    virtual std::string getHeader();
    virtual int getIndex();
    virtual int getKey();
    virtual int getPair();
    virtual int getSlot();
    virtual std::string getString();
    virtual int getValue();
    virtual bool nextAll();
    virtual bool nextValid();

  protected:
    virtual int value() = 0;

    const int lengthPairs;
    int index;
    int val;
};

}

#include "HllPairIterator-internal.hpp"

#endif /* _HLLPAIRITERATOR_HPP_ */
