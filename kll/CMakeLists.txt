target_include_directories(datasketches PUBLIC include)

target_sources_local(datasketches
  PUBLIC
    include/kll_helper.hpp
    include/kll_sketch.hpp
    include/kll_quantile_calculator.hpp
)