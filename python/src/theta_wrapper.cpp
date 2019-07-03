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

#include <theta_sketch.hpp>
#include <theta_union.hpp>
#include <theta_intersection.hpp>
#include <theta_a_not_b.hpp>

#include <pybind11/pybind11.h>
#include <sstream>

namespace py = pybind11;

namespace datasketches {
namespace python {

update_theta_sketch update_theta_sketch_factory(uint8_t lg_k, double p, uint64_t seed) {
  update_theta_sketch::builder builder;
  builder.set_lg_k(lg_k);
  builder.set_p(p);
  builder.set_seed(seed);
  return builder.build();
}

theta_union theta_union_factory(uint8_t lg_k, double p, uint64_t seed) {
  theta_union::builder builder;
  builder.set_lg_k(lg_k);
  builder.set_p(p);
  builder.set_seed(seed);
  return builder.build();
}

theta_sketch* theta_sketch_deserialize(py::bytes skBytes,
                                       uint64_t seed = update_theta_sketch::builder::DEFAULT_SEED) {
  std::string skStr = skBytes; // implicit cast  
  return theta_sketch::deserialize(skStr.c_str(), skStr.length(), seed).release();
}

py::object theta_sketch_serialize(const theta_sketch& sk) {
  auto serResult = sk.serialize();
  return py::bytes((char*)serResult.first.get(), serResult.second);
}

std::string theta_sketch_to_string(const theta_sketch& sk,
                                   bool print_items = false) {
  std::ostringstream ss;
  sk.to_stream(ss, print_items);
  return ss.str();
}

uint16_t theta_sketch_get_seed_hash(const theta_sketch& sk) {
  return sk.get_seed_hash();
}

update_theta_sketch update_theta_sketch_deserialize(py::bytes skBytes,
                                                     uint64_t seed = update_theta_sketch::builder::DEFAULT_SEED) {
  std::string skStr = skBytes; // implicit cast  
  return update_theta_sketch::deserialize(skStr.c_str(), skStr.length(), seed);
}

compact_theta_sketch compact_theta_sketch_deserialize(py::bytes skBytes,
                                                      uint64_t seed = update_theta_sketch::builder::DEFAULT_SEED) {
  std::string skStr = skBytes; // implicit cast  
  return compact_theta_sketch::deserialize(skStr.c_str(), skStr.length(), seed);
}

}
}

namespace dspy = datasketches::python;

void init_theta(py::module &m) {
  using namespace datasketches;

  py::class_<theta_sketch>(m, "theta_sketch")
    .def("serialize", &dspy::theta_sketch_serialize)
    .def_static("deserialize", &dspy::theta_sketch_deserialize, py::arg("bytes"), py::arg("seed")=update_theta_sketch::builder::DEFAULT_SEED)
    .def("__str__", &dspy::theta_sketch_to_string, py::arg("print_items")=false)
    .def("to_string", &dspy::theta_sketch_to_string, py::arg("print_items")=false)
    .def("is_empty", &theta_sketch::is_empty)
    .def("get_estimate", &theta_sketch::get_estimate)
    .def("get_upper_bound", &theta_sketch::get_upper_bound, py::arg("num_std_devs"))
    .def("get_lower_bound", &theta_sketch::get_lower_bound, py::arg("num_std_devs"))
    .def("is_estimation_mode", &theta_sketch::is_estimation_mode)
    .def("get_theta", &theta_sketch::get_theta)
    .def("get_num_retained", &theta_sketch::get_num_retained)
    .def("get_seed_hash", &dspy::theta_sketch_get_seed_hash)
    .def("is_ordered", &theta_sketch::is_ordered)
  ;

  py::class_<update_theta_sketch, theta_sketch>(m, "update_theta_sketch")
    .def(py::init(&dspy::update_theta_sketch_factory),
         py::arg("lg_k")=update_theta_sketch::builder::DEFAULT_LG_K, py::arg("p")=1.0, py::arg("seed")=update_theta_sketch::builder::DEFAULT_SEED)
    .def(py::init<const update_theta_sketch&>())
    .def("update", (void (update_theta_sketch::*)(int64_t)) &update_theta_sketch::update, py::arg("datum"))
    .def("update", (void (update_theta_sketch::*)(double)) &update_theta_sketch::update, py::arg("datum"))
    .def("update", (void (update_theta_sketch::*)(const std::string&)) &update_theta_sketch::update, py::arg("datum"))
    .def("compact", &update_theta_sketch::compact, py::arg("ordered")=true)
    .def_static("deserialize", &dspy::update_theta_sketch_deserialize)
  ;

  py::class_<compact_theta_sketch, theta_sketch>(m, "compact_theta_sketch")
    .def(py::init<const compact_theta_sketch&>())
    .def(py::init<const theta_sketch&, bool>())
    .def_static("deserialize", &dspy::compact_theta_sketch_deserialize)
  ;

  py::class_<theta_union>(m, "theta_union")
    .def(py::init(&dspy::theta_union_factory),
         py::arg("lg_k")=update_theta_sketch::builder::DEFAULT_LG_K, py::arg("p")=1.0, py::arg("seed")=update_theta_sketch::builder::DEFAULT_SEED)
    .def("update", &theta_union::update, py::arg("sketch"))
    .def("get_result", &theta_union::get_result, py::arg("ordered")=true)
  ;

  py::class_<theta_intersection>(m, "theta_intersection")
    .def(py::init<uint64_t>(), py::arg("seed")=update_theta_sketch::builder::DEFAULT_SEED)
    .def(py::init<const theta_intersection&>())
    .def("update", &theta_intersection::update, py::arg("sketch"))
    .def("get_result", &theta_intersection::get_result, py::arg("ordered")=true)
    .def("has_result", &theta_intersection::has_result)
  ;

  py::class_<theta_a_not_b>(m, "theta_a_not_b")
    .def(py::init<uint64_t>(), py::arg("seed")=update_theta_sketch::builder::DEFAULT_SEED)
    .def("compute", &theta_a_not_b::compute, py::arg("a"), py::arg("b"), py::arg("ordered")=true)
  ;
}
