//
// Created by geonove on 8/30/25.
//

#ifndef INDEX_MAPPING_HPP
#define INDEX_MAPPING_HPP
#include <cstdint>
#include <iosfwd>

namespace datasketches {

enum class IndexMappingLayout : uint8_t {
  LOG,
  LOG_LINEAR,
  LOG_QUADRATIC,
  LOG_CUBIC,
  LOG_QUARTIC,
};

class IndexMapping {
public:
  virtual int index(double value) = 0;

  virtual double value(int index) = 0;

  virtual double lowerBound(int index) = 0;

  virtual double upperBound(int index) = 0;

  virtual double relativeAccuracy() = 0;

  virtual double minIndexableValue() = 0;

  virtual double maxIndexableValue() = 0;

  virtual void encode(std::ostream& os) = 0;

  template<IndexMappingLayout layout>
  static IndexMapping* decode(std::istream& is);

  virtual ~IndexMapping() = default;
};

}

#endif //INDEX_MAPPING_HPP
