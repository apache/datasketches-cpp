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

#include "density_sketch.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <vector>

namespace py = pybind11;

namespace datasketches {

namespace python {

template<typename T>
py::list density_sketch_get_coreset(const density_sketch<T>& sketch) {
  py::list list(sketch.get_num_retained());
  unsigned i = 0;
  for (auto pair: sketch) {
    list[i++] = py::make_tuple(pair.first, pair.second);
  }
  return list;
}

}
}

namespace dspy = datasketches::python;

template<typename T>
void bind_density_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<density_sketch<T>>(m, name)
    .def(py::init<uint16_t, uint32_t>(), py::arg("k"), py::arg("dim"))
    .def("update", static_cast<void (density_sketch<T>::*)(const std::vector<T>&)>(&density_sketch<T>::update),
        "Updates the sketch with the given vector")
    .def("update", static_cast<void (density_sketch<T>::*)(std::vector<T>&&)>(&density_sketch<T>::update),
        "Updates the sketch with the given vector")
    .def("merge", static_cast<void (density_sketch<T>::*)(const density_sketch<T>&)>(&density_sketch<T>::merge), py::arg("sketch"),
        "Merges the provided sketch into this one")
    .def("is_empty", &density_sketch<T>::is_empty,
        "Returns True if the sketch is empty, otherwise False")
    .def("get_k", &density_sketch<T>::get_k,
        "Returns the configured parameter k")
    .def("get_dim", &density_sketch<T>::get_dim,
        "Returns the configured parameter dim")
    .def("get_n", &density_sketch<T>::get_n,
        "Returns the length of the input stream")
    .def("get_num_retained", &density_sketch<T>::get_num_retained,
        "Returns the number of retained items (samples) in the sketch")
    .def("is_estimation_mode", &density_sketch<T>::is_estimation_mode,
        "Returns True if the sketch is in estimation mode, otherwise False")
    .def("get_estimate", &density_sketch<T>::get_estimate, py::arg("point"),
        "Returns an approximate density at the given point")
    .def("get_coreset", &dspy::density_sketch_get_coreset<T>,
        "Returns the retained samples with weights")
    .def("__str__", &density_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
        "Produces a string summary of the sketch")
    .def("to_string", &density_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
        "Produces a string summary of the sketch")
    ;
}

void init_density(py::module &m) {
  bind_density_sketch<float>(m, "density_floats_sketch");
  bind_density_sketch<double>(m, "density_doubles_sketch");
}
