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

#include "frequent_items_sketch.hpp"

#include <pybind11/pybind11.h>
#include <sstream>

namespace py = pybind11;

namespace datasketches {
namespace python {

template<typename T>
frequent_items_sketch<T> fi_sketch_deserialize(py::bytes skBytes) {
  std::string skStr = skBytes; // implicit cast  
  return frequent_items_sketch<T>::deserialize(skStr.c_str(), skStr.length());
}

template<typename T>
py::object fi_sketch_serialize(const frequent_items_sketch<T>& sk) {
  auto serResult = sk.serialize();
  return py::bytes((char*)serResult.first.get(), serResult.second);
}

// maybe possible to disambiguate the static vs method get_epsilon calls, but
// this is easier for now
template<typename T>
double fi_sketch_get_generic_epsilon(uint8_t lg_max_map_size) {
  return frequent_items_sketch<T>::get_epsilon(lg_max_map_size);
}

template<typename T>
py::list fi_sketch_get_frequent_items(const frequent_items_sketch<T>& sk,
                                   frequent_items_error_type err_type,
                                   uint64_t threshold = 0) {
  if (threshold == 0) { threshold = sk.get_maximum_error(); }

  py::list list;
  auto items = sk.get_frequent_items(err_type, threshold);
  for (auto iter = items.begin(); iter != items.end(); ++iter) {
    py::tuple t = py::make_tuple(iter->get_item(),
                                 iter->get_estimate(),
                                 iter->get_lower_bound(),
                                 iter->get_upper_bound());
    list.append(t);
  }
  return list;
}

template<typename T>
std::string fi_sketch_to_string(const frequent_items_sketch<T>& sk,
                              bool print_items = false) {
  std::ostringstream ss;
  sk.to_stream(ss, print_items);
  return ss.str();
}

}
}

namespace dspy = datasketches::python;

template<typename T>
void bind_fi_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<frequent_items_sketch<T>>(m, name)
    .def(py::init<uint8_t>(), py::arg("lg_max_k"))
    .def("__str__", &dspy::fi_sketch_to_string<T>, py::arg("print_items")=false)
    .def("to_string", &dspy::fi_sketch_to_string<T>, py::arg("print_items")=false)
    .def("update", (void (frequent_items_sketch<T>::*)(const T&, uint64_t)) &frequent_items_sketch<T>::update, py::arg("item"), py::arg("weight")=1)
    .def("get_frequent_items", &dspy::fi_sketch_get_frequent_items<T>, py::arg("err_type"), py::arg("threshold")=0)
    .def("merge", &frequent_items_sketch<T>::merge)
    .def("is_empty", &frequent_items_sketch<T>::is_empty)
    .def("get_num_active_items", &frequent_items_sketch<T>::get_num_active_items)
    .def("get_total_weight", &frequent_items_sketch<T>::get_total_weight)
    .def("get_estimate", &frequent_items_sketch<T>::get_estimate, py::arg("item"))
    .def("get_lower_bound", &frequent_items_sketch<T>::get_lower_bound, py::arg("item"))
    .def("get_upper_bound", &frequent_items_sketch<T>::get_upper_bound, py::arg("item"))
    .def("get_sketch_epsilon", (double (frequent_items_sketch<T>::*)(void) const) &frequent_items_sketch<T>::get_epsilon)
    .def_static("get_epsilon_for_lg_size", &dspy::fi_sketch_get_generic_epsilon<T>, py::arg("lg_max_map_size"))
    .def_static("get_apriori_error", &frequent_items_sketch<T>::get_apriori_error, py::arg("lg_max_map_size"), py::arg("estimated_total_weight"))
    .def("get_serialized_size_bytes", &frequent_items_sketch<T>::get_serialized_size_bytes)
    .def("serialize", &dspy::fi_sketch_serialize<T>)
    .def_static("deserialize", &dspy::fi_sketch_deserialize<T>)
    ;
}

void init_fi(py::module &m) {
  using namespace datasketches;

  py::enum_<frequent_items_error_type>(m, "frequent_items_error_type")
    .value("NO_FALSE_POSITIVES", NO_FALSE_POSITIVES)
    .value("NO_FALSE_NEGATIVES", NO_FALSE_NEGATIVES)
    .export_values();

  bind_fi_sketch<std::string>(m, "frequent_strings_sketch");
}
