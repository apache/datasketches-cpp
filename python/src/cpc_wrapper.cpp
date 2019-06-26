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


#include "cpc_sketch.hpp"
#include "cpc_union.hpp"
#include "cpc_common.hpp"

#include <pybind11/pybind11.h>
#include <sstream>

namespace py = pybind11;

namespace datasketches {
namespace python {

cpc_sketch* CpcSketch_deserialize(py::bytes skBytes) {
  std::string skStr = skBytes; // implicit cast
  cpc_sketch_unique_ptr sk = cpc_sketch::deserialize(skStr.c_str(), skStr.length());
  return sk.release();
}

py::object CpcSketch_serialize(const cpc_sketch& sk) {
  auto serResult = sk.serialize();
  return py::bytes((char*)serResult.first.get(), serResult.second);
}

std::string CpcSketch_toString(const cpc_sketch& sk) {
  std::ostringstream ss;
  ss << sk;
  return ss.str();
}

cpc_sketch* CpcUnion_getResult(const cpc_union& u) {
  auto sk = u.get_result();
  return sk.release();
}

}
}

namespace dspy = datasketches::python;

void init_cpc(py::module &m) {
  using namespace datasketches;

  py::class_<cpc_sketch>(m, "cpc_sketch")
    .def(py::init<const cpc_sketch&>())
    .def(py::init<uint8_t>())
    .def(py::init<uint8_t, uint64_t>())
    .def("__str__", &dspy::CpcSketch_toString)
    .def("serialize", &dspy::CpcSketch_serialize)
    .def_static("deserialize", &dspy::CpcSketch_deserialize, py::return_value_policy::take_ownership)
    .def<void (cpc_sketch::*)(uint64_t)>("update", &cpc_sketch::update)
    .def<void (cpc_sketch::*)(int64_t)>("update", &cpc_sketch::update)
    .def<void (cpc_sketch::*)(double)>("update", &cpc_sketch::update)
    .def<void (cpc_sketch::*)(const std::string&)>("update", &cpc_sketch::update)
    .def("is_empty", &cpc_sketch::is_empty)
    .def("get_estimate", &cpc_sketch::get_estimate)
    .def("get_lower_bound", &cpc_sketch::get_lower_bound)
    .def("get_upper_bound", &cpc_sketch::get_upper_bound)
    .def("get_estimate", &cpc_sketch::get_estimate)
    ;

  py::class_<cpc_union>(m, "cpc_union")
    .def(py::init<const cpc_union&>())
    .def(py::init<uint8_t>())
    .def(py::init<uint8_t, uint64_t>())
    .def("update", &cpc_union::update)
    .def("get_result", &dspy::CpcUnion_getResult, py::return_value_policy::take_ownership)
    ;
}
