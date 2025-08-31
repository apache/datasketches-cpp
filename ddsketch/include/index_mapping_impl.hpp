//
// Created by geonove on 8/30/25.
//

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
