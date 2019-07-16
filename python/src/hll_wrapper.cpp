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

#include "hll.hpp"

#include <pybind11/pybind11.h>

namespace py = pybind11;

namespace datasketches {
namespace python {

HllSketch<> hll_sketch_deserialize(py::bytes skBytes) {
  std::string skStr = skBytes; // implicit cast  
  return HllSketch<>::deserialize(skStr.c_str(), skStr.length());
}

py::object hll_sketch_serialize_compact(const HllSketch<>& sk) {
  auto serResult = sk.serializeCompact();
  return py::bytes((char*)serResult.first.get(), serResult.second);
}

py::object hll_sketch_serialize_updatable(const HllSketch<>& sk) {
  auto serResult = sk.serializeUpdatable();
  return py::bytes((char*)serResult.first.get(), serResult.second);
}

HllUnion<> hll_union_deserialize(py::bytes uBytes) {
  std::string uStr = uBytes; // implicit cast
  return HllUnion<>::deserialize(uStr.c_str(), uStr.length());
}

py::object hll_union_serialize_compact(const HllUnion<>& u) {
  auto serResult = u.serializeCompact();
  return py::bytes((char*)serResult.first.get(), serResult.second);
}

py::object hll_union_serialize_updatable(const HllUnion<>& u) {
  auto serResult = u.serializeUpdatable();
  return py::bytes((char*)serResult.first.get(), serResult.second);
}

}
}

namespace dspy = datasketches::python;

void init_hll(py::module &m) {
  using namespace datasketches;

  py::enum_<TgtHllType>(m, "tgt_hll_type", "Target HLL flavor")
    .value("HLL_4", HLL_4)
    .value("HLL_6", HLL_6)
    .value("HLL_8", HLL_8)
    .export_values();

  py::class_<HllSketch<>>(m, "hll_sketch")
    .def(py::init<int>(), py::arg("lg_k"))
    .def(py::init<int, TgtHllType>(), py::arg("lg_k"), py::arg("tgt_hll_type"))
    .def(py::init<int, TgtHllType, bool>(), py::arg("lg_k"), py::arg("tgt_hll_type"), py::arg("start_max_size")=false)
    .def_static("deserialize", &dspy::hll_sketch_deserialize)
    .def("serialize_compact", &dspy::hll_sketch_serialize_compact)
    .def("serialize_updatable", &dspy::hll_sketch_serialize_updatable)
    .def("to_string", (std::string (HllSketch<>::*)(bool,bool,bool,bool) const) &HllSketch<>::to_string,
         py::arg("summary")=true, py::arg("detail")=false, py::arg("aux_detail")=false, py::arg("all")=false)
    .def("__str__", (std::string (HllSketch<>::*)(bool,bool,bool,bool) const) &HllSketch<>::to_string,
         py::arg("summary")=true, py::arg("detail")=false, py::arg("aux_detail")=false, py::arg("all")=false)
    .def_property_readonly("lg_config_k", &HllSketch<>::getLgConfigK)
    .def_property_readonly("tgt_hll_type", &HllSketch<>::getTgtHllType)
    .def("get_estimate", &HllSketch<>::getEstimate)
    .def("get_composite_estimate", &HllSketch<>::getCompositeEstimate)
    .def("get_lower_bound", &HllSketch<>::getLowerBound, py::arg("num_std_devs"))
    .def("get_upper_bound", &HllSketch<>::getUpperBound, py::arg("num_std_devs"))
    .def("is_compact", &HllSketch<>::isCompact)
    .def("is_empty", &HllSketch<>::isEmpty)
    .def("get_updatable_serialization_bytes", &HllSketch<>::getUpdatableSerializationBytes)
    .def("get_compact_serialization_bytes", &HllSketch<>::getCompactSerializationBytes)
    .def("reset", &HllSketch<>::reset)
    .def("update", (void (HllSketch<>::*)(int64_t)) &HllSketch<>::update, py::arg("datum"))
    .def("update", (void (HllSketch<>::*)(double)) &HllSketch<>::update, py::arg("datum"))
    .def("update", (void (HllSketch<>::*)(const std::string&)) &HllSketch<>::update, py::arg("datum"))
    .def_static("get_max_updatable_serialization_bytes", &HllSketch<>::getMaxUpdatableSerializationBytes,
         py::arg("lg_k"), py::arg("tgt_hll_type"))
    .def_static("get_rel_err", &HllSketch<>::getRelErr,
         py::arg("upper_bound"), py::arg("unioned"), py::arg("lg_k"), py::arg("num_std_devs"))
    ;

  py::class_<HllUnion<>>(m, "hll_union")
    .def(py::init<int>(), py::arg("lg_max_k"))
    .def_static("deserialize", &dspy::hll_union_deserialize)
    .def("serialize_compact", &dspy::hll_union_serialize_compact)
    .def("serialize_updatable", &dspy::hll_union_serialize_updatable)
    .def("to_string", (std::string (HllUnion<>::*)(bool,bool,bool,bool) const) &HllUnion<>::to_string,
         py::arg("summary")=true, py::arg("detail")=false, py::arg("aux_detail")=false, py::arg("all")=false)
    .def("__str__", (std::string (HllUnion<>::*)(bool,bool,bool,bool) const) &HllUnion<>::to_string,
         py::arg("summary")=true, py::arg("detail")=false, py::arg("aux_detail")=false, py::arg("all")=false)
    .def_property_readonly("lg_config_k", &HllUnion<>::getLgConfigK)
    .def_property_readonly("tgt_hll_type", &HllUnion<>::getTgtHllType)
    .def("get_estimate", &HllUnion<>::getEstimate)
    .def("get_composite_estimate", &HllUnion<>::getCompositeEstimate)
    .def("get_lower_bound", &HllUnion<>::getLowerBound, py::arg("num_std_devs"))
    .def("get_upper_bound", &HllUnion<>::getUpperBound, py::arg("num_std_devs"))
    .def("is_compact", &HllUnion<>::isCompact)
    .def("is_empty", &HllUnion<>::isEmpty)
    .def("get_updatable_serialization_bytes", &HllUnion<>::getUpdatableSerializationBytes)
    .def("get_compact_serialization_bytes", &HllUnion<>::getCompactSerializationBytes)
    .def("reset", &HllUnion<>::reset)
    .def("get_result", &HllUnion<>::getResult, py::arg("tgt_hll_type")=HLL_4)
    .def<void (HllUnion<>::*)(const HllSketch<>&)>("update", &HllUnion<>::update, py::arg("sketch"))
    .def<void (HllUnion<>::*)(int64_t)>("update", &HllUnion<>::update, py::arg("datum"))
    .def<void (HllUnion<>::*)(double)>("update", &HllUnion<>::update, py::arg("datum"))
    .def<void (HllUnion<>::*)(const std::string&)>("update", &HllUnion<>::update, py::arg("datum"))
    .def_static("get_max_serialization_bytes", &HllUnion<>::getMaxSerializationBytes, py::arg("lg_k"))
    .def_static("get_rel_err", &HllUnion<>::getRelErr,
         py::arg("upper_bound"), py::arg("unioned"), py::arg("lg_k"), py::arg("num_std_devs"))
    ;
}
