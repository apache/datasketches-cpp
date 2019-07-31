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

#ifndef _PAIRITERATOR_HPP_
#define _PAIRITERATOR_HPP_

#include <string>
#include <functional>
#include <memory>

namespace datasketches {

template<typename A = std::allocator<char>>
class PairIterator {
  public:
    virtual std::string getHeader() = 0;

    virtual int getIndex() = 0;
    virtual int getKey() = 0;
    virtual int getPair() = 0;
    virtual int getSlot() = 0;

    virtual std::string getString() = 0;

    virtual int getValue() = 0;
    virtual bool nextAll() = 0;
    virtual bool nextValid() = 0;

    virtual ~PairIterator() = default;
};

template<typename A = std::allocator<char>>
using PairIterator_with_deleter = std::unique_ptr<PairIterator<A>, std::function<void(PairIterator<A>*)>>;

}

#endif /* _PAIRITERATOR_HPP_ */
