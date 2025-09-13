//
// Created by Andrea Novellini on 13/09/2025.
//

#ifndef DATASKETCHES_INDEX_MAPPING_DECODE_HPP
#define DATASKETCHES_INDEX_MAPPING_DECODE_HPP

#include "index_mapping.hpp"
#include "logarithmic_mapping.hpp"
#include "quadratically_interpolated_mapping.hpp"

namespace datasketches {

template<class Derived>
template<IndexMappingLayout layout>
IndexMapping<Derived>* IndexMapping<Derived>::decode(std::istream& is) const {
  const auto gamma = read<double>(is);
  const auto index_offset = read<double>(is);

  switch (layout) {
    case IndexMappingLayout::LOG:
      return new LogarithmicMapping(gamma, index_offset);
    case IndexMappingLayout::LOG_LINEAR:
      return new LinearlyInterpolatedMapping(gamma, index_offset);
    case IndexMappingLayout::LOG_QUADRATIC:
      return new QuadraticallyInterpolatedMapping(gamma, index_offset);
    case IndexMappingLayout::LOG_QUARTIC:
      return new QuadraticallyInterpolatedMapping(gamma, index_offset);
    default:
      throw std::invalid_argument("Invalid index mapping layout");
  }
}
}

#endif //DATASKETCHES_INDEX_MAPPING_DECODE_HPP