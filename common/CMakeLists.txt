target_include_directories(datasketches PUBLIC include)

target_sources_local(datasketches
  PRIVATE
    include/MurmurHash3.h
)