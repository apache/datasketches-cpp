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
  template<typename... Args>
  static std::unique_ptr<IndexMapping> new_mapping(Args&&... args)
  {
    return std::make_unique<IndexMapping>(std::forward<Args>(args)...);
  }
};
}

#endif //INDEX_MAPPING_FACTORY_HPP
