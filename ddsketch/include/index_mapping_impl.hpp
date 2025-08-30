//
// Created by geonove on 8/30/25.
//

#ifndef INDEX_MAPPING_IMPL_HPP
#define INDEX_MAPPING_IMPL_HPP

#include <istream>

#include "index_mapping.hpp"
#include "linearly_interpolated_mapping.hpp"

namespace datasketches {

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
