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

#include "common_defs.hpp"
#include "index_mapping.hpp"

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

template<class Derived>
Derived IndexMapping<Derived>::deserialize(std::istream &is) {
  const auto gamma = read<double>(is);
  const auto index_offset = read<double>(is);


  return Derived(gamma, index_offset);
}

template<class Derived>
Derived& IndexMapping<Derived>::derived() {
  return *static_cast<Derived*>(this);
}

template<class Derived>
const Derived& IndexMapping<Derived>::derived() const {
  return *static_cast<const Derived*>(this);
}

template<class Derived>
void IndexMapping<Derived>::serialize(std::ostream &os) const {
  derived().serialize(os);
}

template<class Derived>
double IndexMapping<Derived>::get_relative_accuracy() const {
  return derived().get_relative_accuracy();
}
}


#endif //INDEX_MAPPING_IMPL_HPP
