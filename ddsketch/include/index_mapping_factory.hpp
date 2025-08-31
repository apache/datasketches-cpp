//
// Created by Andrea Novellini on 31/08/2025.
//

#ifndef INDEX_MAPPING_FACTORY_HPP
#define INDEX_MAPPING_FACTORY_HPP

#include <memory>


namespace datasketches {
template<typename IndexMapping>
class index_mapping_factory {
public:
  static std::unique_ptr<IndexMapping> new_mapping(auto... Args)
  {
    return std::make_unique<IndexMapping>(Args...);
  }
};
}

#endif //INDEX_MAPPING_FACTORY_HPP
