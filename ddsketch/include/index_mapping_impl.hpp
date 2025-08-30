//
// Created by geonove on 8/30/25.
//

#ifndef INDEX_MAPPING_IMPL_HPP
#define INDEX_MAPPING_IMPL_HPP

#include "index_mapping.hpp"

namespace datasketches {
template<class Derived, IndexMappingLayout Layout>
IndexMapping<Derived, Layout>* IndexMapping<Derived, Layout>::decode() {
  switch (Layout) {
    case IndexMappingLayout::LOG:
      return nullptr;
    default:
      return nullptr;
  }
}

}


#endif //INDEX_MAPPING_IMPL_HPP
