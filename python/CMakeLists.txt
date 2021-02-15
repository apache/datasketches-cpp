# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# TODO: Can we force python version >= 3.0?
if (MSVC)
  set(PYBIND11_CPP_STANDARD /std:c++11)
else()
  set(PYBIND11_CPP_STANDARD -std=c++11)
endif()

add_subdirectory(pybind11)

pybind11_add_module(python MODULE EXCLUDE_FROM_ALL SYSTEM THIN_LTO)

target_link_libraries(python
  PRIVATE
    common
    hll
    kll
    cpc
    fi
    theta
    sampling
    req
    pybind11::module
)

set_target_properties(python PROPERTIES
  PREFIX ""
  OUTPUT_NAME datasketches
)

# ensure we make a .so on Mac rather than .dylib
if (${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set_target_properties(python PROPERTIES SUFFIX ".so")
endif()

target_sources(python
  PRIVATE
    src/datasketches.cpp
    src/hll_wrapper.cpp
    src/kll_wrapper.cpp
    src/cpc_wrapper.cpp
    src/fi_wrapper.cpp
    src/theta_wrapper.cpp
    src/vo_wrapper.cpp
    src/req_wrapper.cpp
    src/vector_of_kll.cpp
)
