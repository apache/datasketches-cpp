target_include_directories(datasketches PUBLIC include)

target_sources_local(datasketches
  PUBLIC
    include/cpc_sketch.hpp
    include/cpc_union.hpp
    include/cpc_common.hpp
  PRIVATE
    src/compressionData.data
    src/cpc_sketch.cpp
    src/fm85.cpp
    src/fm85Compression.cpp
    src/fm85Confidence.cpp
    src/fm85Merging.cpp
    src/fm85Util.cpp
    src/iconEstimator.cpp
    src/u32Table.cpp
    include/common.h
    include/fm85.h
    include/fm85Compression.h
    include/fm85Confidence.h
    include/fm85Merging.h
    include/fm85Util.h
    include/iconEstimator.h
    include/u32Table.h
)