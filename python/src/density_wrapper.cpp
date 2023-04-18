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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <vector>

#include "kernel_function.hpp"
#include "density_sketch.hpp"

namespace py = pybind11;

template<typename T, typename K>
void bind_density_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<density_sketch<T, K>>(m, name)
    .def(
        py::init([](uint16_t k, uint32_t dim, std::shared_ptr<kernel_function> kernel) {
          kernel_function_holder holder(kernel);
          return density_sketch<T, K>(k, dim, holder);
        }),
        py::arg("k"), py::arg("dim"), py::arg("kernel"))
    .def("update", static_cast<void (density_sketch<T, K>::*)(const std::vector<T>&)>(&density_sketch<T, K>::update),
        "Updates the sketch with the given vector")
    .def("merge", static_cast<void (density_sketch<T, K>::*)(const density_sketch<T, K>&)>(&density_sketch<T, K>::merge), py::arg("sketch"),
        "Merges the provided sketch into this one")
    .def("is_empty", &density_sketch<T, K>::is_empty,
        "Returns True if the sketch is empty, otherwise False")
    .def("get_k", &density_sketch<T, K>::get_k,
        "Returns the configured parameter k")
    .def("get_dim", &density_sketch<T, K>::get_dim,
        "Returns the configured parameter dim")
    .def("get_n", &density_sketch<T, K>::get_n,
        "Returns the length of the input stream")
    .def("get_num_retained", &density_sketch<T, K>::get_num_retained,
        "Returns the number of retained items (samples) in the sketch")
    .def("is_estimation_mode", &density_sketch<T, K>::is_estimation_mode,
        "Returns True if the sketch is in estimation mode, otherwise False")
    .def("get_estimate", &density_sketch<T, K>::get_estimate, py::arg("point"),
        "Returns an approximate density at the given point")
    .def("__str__", &density_sketch<T, K>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
        "Produces a string summary of the sketch")
    .def("to_string", &density_sketch<T, K>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
        "Produces a string summary of the sketch")
    .def("__iter__", [](const density_sketch<T, K>& s){ return py::make_iterator(s.begin(), s.end()); })
    .def("serialize",
        [](const density_sketch<T, K>& sk) {
          auto bytes = sk.serialize();
          return py::bytes(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        },
        "Serializes the sketch into a bytes object"
    )
    .def_static(
        "deserialize",
        [](const std::string& bytes, std::shared_ptr<kernel_function> kernel) {
          kernel_function_holder holder(kernel);
          return density_sketch<T, K>::deserialize(bytes.data(), bytes.size(), holder);
        },
        py::arg("bytes"), py::arg("kernel"),
        "Reads a bytes object and returns the corresponding density_sketch"
    );;
}

void init_density(py::module &m) {
  using namespace datasketches;

  // generic kernel function
  py::class_<kernel_function, KernelFunction, std::shared_ptr<kernel_function>>(m, "KernelFunction")
    .def(py::init())
    .def("__call__", &kernel_function::operator(), py::arg("a"), py::arg("b"))
    ;

  // the old sketch names  can almost be defined, but the kernel_function_holder won't work in init()
  //bind_density_sketch<float, gaussian_kernel<float>>(m, "density_floats_sketch");
  //bind_density_sketch<double, gaussian_kernel<double>>(m, "density_doubles_sketch");
  bind_density_sketch<double, kernel_function_holder>(m, "_density_sketch");
}
