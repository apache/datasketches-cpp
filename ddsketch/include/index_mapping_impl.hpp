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

#ifndef INDEX_MAPPING_IMPL_HPP
#define INDEX_MAPPING_IMPL_HPP

#include <istream>

#include "index_mapping.hpp"
#include "linearly_interpolated_mapping.hpp"

namespace datasketches {

inline std::ostream& operator<<(std::ostream& os, const IndexMappingLayout& obj) {
  switch (obj) {
    case IndexMappingLayout::LOG:
      return os << "LOG";
    case IndexMappingLayout::LOG_LINEAR:
      return os << "LOG_LINEAR";
    case IndexMappingLayout::LOG_QUADRATIC:
      return os << "LOG_QUADRATIC";
    case IndexMappingLayout::LOG_CUBIC:
      return os << "LOG_CUBIC";
    case IndexMappingLayout::LOG_QUARTIC:
      return os << "LOG_QUARTIC";
    default:
      return os << "INVALID";
  }
}

template<IndexMappingLayout layout>
IndexMapping* IndexMapping::decode(std::istream& is) {
  switch (layout) {
    case IndexMappingLayout::LOG_LINEAR:
      return nullptr;
    default:
      return nullptr;
  }
}

}


#endif //INDEX_MAPPING_IMPL_HPP
