/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "kll_sketch.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sstream>

namespace py = pybind11;

namespace datasketches {
namespace python {

template<typename T>
kll_sketch<T> kll_sketch_deserialize(py::bytes skBytes) {
  std::string skStr = skBytes; // implicit cast  
  return kll_sketch<T>::deserialize(skStr.c_str(), skStr.length());
}

template<typename T>
py::object kll_sketch_serialize(const kll_sketch<T>& sk) {
  auto serResult = sk.serialize();
  return py::bytes((char*)serResult.first.get(), serResult.second);
}

// maybe possible to disambiguate the static vs method rank error calls, but
// this is easier for now
template<typename T>
double kll_sketch_generic_normalized_rank_error(uint16_t k, bool pmf) {
  return kll_sketch<T>::get_normalized_rank_error(k, pmf);
}

template<typename T>
py::list kll_sketch_get_quantiles(const kll_sketch<T>& sk,
                                std::vector<double>& fractions) {
  size_t nQuantiles = fractions.size();
  auto result = sk.get_quantiles(&fractions[0], nQuantiles);

  // returning as std::vector<> would copy values to a list anyway
  py::list list(nQuantiles);
  for (int i = 0; i < nQuantiles; ++i) {
      list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list kll_sketch_get_pmf(const kll_sketch<T>& sk,
                          std::vector<T>& split_points) {
  size_t nPoints = split_points.size();
  auto result = sk.get_PMF(&split_points[0], nPoints);

  py::list list(nPoints);
  for (int i = 0; i < nPoints; ++i) {
    list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list kll_sketch_get_cdf(const kll_sketch<T>& sk,
                          std::vector<T>& split_points) {
  size_t nPoints = split_points.size();
  auto result = sk.get_CDF(&split_points[0], nPoints);

  py::list list(nPoints);
  for (int i = 0; i < nPoints; ++i) {
    list[i] = result[i];
  }

  return list;
}

template<typename T>
std::string kll_sketch_to_string(const kll_sketch<T>& sk) {
  std::ostringstream ss;
  sk.to_stream(ss);
  return ss.str();
}

}
}

namespace dspy = datasketches::python;

template<typename T>
void bind_kll_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<kll_sketch<T>>(m, name)
    .def(py::init<uint16_t>(), py::arg("k"))
    .def(py::init<const kll_sketch<T>&>())
    .def("update", (void (kll_sketch<T>::*)(const T&)) &kll_sketch<T>::update, py::arg("item"))
    .def("merge", &kll_sketch<T>::merge, py::arg("sketch"))
    .def("__str__", &dspy::kll_sketch_to_string<T>)
    .def("is_empty", &kll_sketch<T>::is_empty)
    .def("get_n", &kll_sketch<T>::get_n)
    .def("get_num_retained", &kll_sketch<T>::get_num_retained)
    .def("is_estimation_mode", &kll_sketch<T>::is_estimation_mode)
    .def("get_min_value", &kll_sketch<T>::get_min_value)
    .def("get_max_value", &kll_sketch<T>::get_max_value)
    .def("get_quantile", &kll_sketch<T>::get_quantile, py::arg("fraction"))
    .def("get_quantiles", &dspy::kll_sketch_get_quantiles<T>, py::arg("fractions"))
    .def("get_rank", &kll_sketch<T>::get_rank, py::arg("value"))
    .def("get_pmf", &dspy::kll_sketch_get_pmf<T>, py::arg("split_points"))
    .def("get_cdf", &dspy::kll_sketch_get_cdf<T>, py::arg("split_points"))
    .def("normalized_rank_error", (double (kll_sketch<T>::*)(bool) const) &kll_sketch<T>::get_normalized_rank_error,
         py::arg("as_pmf"))
    .def_static("get_normalized_rank_error", &dspy::kll_sketch_generic_normalized_rank_error<T>,
         py::arg("k"), py::arg("as_pmf"))
    // can't yet get this one to work
    //.def("get_serialized_size_bytes", &kll_sketch<T>::get_serialized_size_bytes)
    .def("serialize", &dspy::kll_sketch_serialize<T>)
    .def_static("deserialize", &dspy::kll_sketch_deserialize<T>)
    ;
}

void init_kll(py::module &m) {
  bind_kll_sketch<int>(m, "kll_ints_sketch");
  bind_kll_sketch<float>(m, "kll_floats_sketch");
}
