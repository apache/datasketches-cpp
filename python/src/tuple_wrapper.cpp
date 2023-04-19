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

#include <memory>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "theta_sketch.hpp"
#include "tuple_sketch.hpp"
#include "tuple_union.hpp"
#include "tuple_intersection.hpp"
#include "tuple_a_not_b.hpp"
#include "theta_jaccard_similarity_base.hpp"
#include "common_defs.hpp"

#include "py_serde.hpp"
#include "tuple_policy.hpp"

namespace py = pybind11;

void init_tuple(py::module &m) {
  using namespace datasketches;

  // generic tuple_policy:
  // * update sketch policy uses create_summary and update_summary
  // * set operation policies all use __call__
  py::class_<tuple_policy, TuplePolicy, std::shared_ptr<tuple_policy>>(m, "TuplePolicy")
    .def(py::init())
    .def("create_summary", &tuple_policy::create_summary)
    .def("update_summary", &tuple_policy::update_summary, py::arg("summary"), py::arg("update"))
    .def("__call__", &tuple_policy::operator(), py::arg("summary"), py::arg("update"))
  ;

  // potentially useful for debugging but not needed as a permanent
  // object type in the library
  /*
  py::class_<tuple_policy_holder>(m, "TuplePolicyHolder")
    .def(py::init<std::shared_ptr<tuple_policy>>(), py::arg("policy"))
    .def("create", &tuple_policy_holder::create, "Creates a new Summary object")
    .def("update", &tuple_policy_holder::update, py::arg("summary"), py::arg("update"),
         "Updates the provided summary using the data in update")
  ;
  */

  using py_tuple_sketch = tuple_sketch<py::object>;
  using py_update_tuple = update_tuple_sketch<py::object, py::object, tuple_policy_holder>;
  using py_compact_tuple = compact_tuple_sketch<py::object>;
  using py_tuple_union = tuple_union<py::object, tuple_policy_holder>;
  using py_tuple_intersection = tuple_intersection<py::object, tuple_policy_holder>;
  using py_tuple_a_not_b = tuple_a_not_b<py::object>;
  using py_tuple_jaccard_similarity = jaccard_similarity_base<tuple_union<py::object, dummy_jaccard_policy>, tuple_intersection<py::object, dummy_jaccard_policy>, pair_extract_key<uint64_t, py::object>>;

  py::class_<py_tuple_sketch>(m, "_tuple_sketch")
    .def("__str__", &py_tuple_sketch::to_string, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("to_string", &py_tuple_sketch::to_string, py::arg("print_items")=false,
         "Produces a string summary of the sketch")
    .def("is_empty", &py_tuple_sketch::is_empty,
         "Returns True if the sketch is empty, otherwise False")
    .def("get_estimate", &py_tuple_sketch::get_estimate,
         "Estimate of the distinct count of the input stream")
    .def("get_upper_bound", static_cast<double (py_tuple_sketch::*)(uint8_t) const>(&py_tuple_sketch::get_upper_bound), py::arg("num_std_devs"),
         "Returns an approximate upper bound on the estimate at standard deviations in {1, 2, 3}")
    .def("get_lower_bound", static_cast<double (py_tuple_sketch::*)(uint8_t) const>(&py_tuple_sketch::get_lower_bound), py::arg("num_std_devs"),
         "Returns an approximate lower bound on the estimate at standard deviations in {1, 2, 3}")
    .def("is_estimation_mode", &py_tuple_sketch::is_estimation_mode,
         "Returns True if sketch is in estimation mode, otherwise False")
    .def("get_theta", &py_tuple_sketch::get_theta,
         "Returns theta (effective sampling rate) as a fraction from 0 to 1")
    .def("get_theta64", &py_tuple_sketch::get_theta64,
         "Returns theta as 64-bit value")
    .def("get_num_retained", &py_tuple_sketch::get_num_retained,
         "Returns the number of items currently in the sketch")
    .def("get_seed_hash", [](const py_tuple_sketch& sk) { return sk.get_seed_hash(); }, // why does regular call not work??
         "Returns a hash of the seed used in the sketch")
    .def("is_ordered", &py_tuple_sketch::is_ordered,
         "Returns True if the sketch entries are sorted, otherwise False")
    .def("__iter__", [](const py_tuple_sketch& s) { return py::make_iterator(s.begin(), s.end()); })
    .def_property_readonly_static("DEFAULT_SEED", [](py::object /* self */) { return DEFAULT_SEED; });
  ;

  py::class_<py_compact_tuple, py_tuple_sketch>(m, "_compact_tuple_sketch")
    .def(py::init<const py_compact_tuple&>(), py::arg("other"))
    .def(py::init<const py_tuple_sketch&, bool>(), py::arg("other"), py::arg("ordered")=true)
    .def(py::init<const theta_sketch&, py::object&>(), py::arg("other"), py::arg("summary"),
         "Creates a compact tuple sketch from a theta sketch using a fixed summary value.")
    .def(
        "serialize",
        [](const py_compact_tuple& sk, py_object_serde& serde) {
          auto bytes = sk.serialize(0, serde);
          return py::bytes(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        }, py::arg("serde"),
        "Serializes the sketch into a bytes object"
    )
    .def_static(
        "deserialize",
        [](const std::string& bytes, py_object_serde& serde, uint64_t seed) {
          return py_compact_tuple::deserialize(bytes.data(), bytes.size(), seed, serde);
        },
        py::arg("bytes"), py::arg("serde"), py::arg("seed")=DEFAULT_SEED,
        "Reads a bytes object and returns the corresponding compact_tuple_sketch"
    );

  py::class_<py_update_tuple, py_tuple_sketch>(m, "_update_tuple_sketch")
    .def(
        py::init([](std::shared_ptr<tuple_policy> policy, uint8_t lg_k, double p, uint64_t seed) {
          tuple_policy_holder holder(policy);
          return py_update_tuple::builder(holder).set_lg_k(lg_k).set_p(p).set_seed(seed).build();
        }),
        py::arg("policy"), py::arg("lg_k")=theta_constants::DEFAULT_LG_K, py::arg("p")=1.0, py::arg("seed")=DEFAULT_SEED
    )
    .def(py::init<const py_update_tuple&>())
    .def("update", static_cast<void (py_update_tuple::*)(int64_t, py::object&)>(&py_update_tuple::update),
         py::arg("datum"), py::arg("value"),
         "Updates the sketch with the given integral item and summary value")
    .def("update", static_cast<void (py_update_tuple::*)(double, py::object&)>(&py_update_tuple::update),
         py::arg("datum"), py::arg("value"),
         "Updates the sketch with the given floating point item and summary value")
    .def("update", static_cast<void (py_update_tuple::*)(const std::string&, py::object&)>(&py_update_tuple::update),
         py::arg("datum"), py::arg("value"),
         "Updates the sketch with the given string item and summary value")
    .def("compact", &py_update_tuple::compact, py::arg("ordered")=true,
         "Returns a compacted form of the sketch, optionally sorting it")
    .def("reset", &py_update_tuple::reset, "Resets the sketch to the initial empty state")
  ;

  py::class_<py_tuple_union>(m, "_tuple_union")
    .def(
        py::init([](std::shared_ptr<tuple_policy> policy, uint8_t lg_k, double p, uint64_t seed) {
          tuple_policy_holder holder(policy);
          return py_tuple_union::builder(holder).set_lg_k(lg_k).set_p(p).set_seed(seed).build();
        }),
        py::arg("policy"), py::arg("lg_k")=theta_constants::DEFAULT_LG_K, py::arg("p")=1.0, py::arg("seed")=DEFAULT_SEED
    )
    .def("update", &py_tuple_union::update<const py_tuple_sketch&>, py::arg("sketch"),
         "Updates the union with the given sketch")
    .def("get_result", &py_tuple_union::get_result, py::arg("ordered")=true,
         "Returns the sketch corresponding to the union result")
    .def("reset", &py_tuple_union::reset,
         "Resets the sketch to the initial empty")
  ;

  py::class_<py_tuple_intersection>(m, "_tuple_intersection")
    .def(
        py::init([](std::shared_ptr<tuple_policy> policy, uint64_t seed) {
          tuple_policy_holder holder(policy);
          return py_tuple_intersection(seed, holder);
        }),
        py::arg("policy"), py::arg("seed")=DEFAULT_SEED)
    .def("update", &py_tuple_intersection::update<const py_tuple_sketch&>, py::arg("sketch"),
         "Intersects the provided sketch with the current intersection state")
    .def("get_result", &py_tuple_intersection::get_result, py::arg("ordered")=true,
         "Returns the sketch corresponding to the intersection result")
    .def("has_result", &py_tuple_intersection::has_result,
         "Returns True if the intersection has a valid result, otherwise False")
  ;

  py::class_<py_tuple_a_not_b>(m, "_tuple_a_not_b")
    .def(py::init<uint64_t>(), py::arg("seed")=DEFAULT_SEED)
    .def(
        "compute",
        &py_tuple_a_not_b::compute<const py_tuple_sketch&, const py_tuple_sketch&>,
        py::arg("a"), py::arg("b"), py::arg("ordered")=true,
        "Returns a sketch with the result of applying the A-not-B operation on the given inputs"
    )
  ;

  py::class_<py_tuple_jaccard_similarity>(m, "_tuple_jaccard_similarity")
    .def_static(
        "jaccard",
        [](const py_tuple_sketch& sketch_a, const py_tuple_sketch& sketch_b, uint64_t seed) {
          return py_tuple_jaccard_similarity::jaccard(sketch_a, sketch_b, seed);
        },
        py::arg("sketch_a"), py::arg("sketch_b"), py::arg("seed")=DEFAULT_SEED,
        "Returns a list with {lower_bound, estimate, upper_bound} of the Jaccard similarity between sketches"
    )
    .def_static(
        "exactly_equal",
        &py_tuple_jaccard_similarity::exactly_equal<const py_tuple_sketch&, const py_tuple_sketch&>,
        py::arg("sketch_a"), py::arg("sketch_b"), py::arg("seed")=DEFAULT_SEED,
        "Returns True if sketch_a and sketch_b are equivalent, otherwise False"
    )
    .def_static(
        "similarity_test",
        &py_tuple_jaccard_similarity::similarity_test<const py_tuple_sketch&, const py_tuple_sketch&>,
        py::arg("actual"), py::arg("expected"), py::arg("threshold"), py::arg("seed")=DEFAULT_SEED,
        "Tests similarity of an actual sketch against an expected sketch. Computes the lower bound of the Jaccard "
        "index J_{LB} of the actual and expected sketches. If J_{LB} >= threshold, then the sketches are considered "
        "to be similar with a confidence of 97.7% and returns True, otherwise False.")
    .def_static(
        "dissimilarity_test",
        &py_tuple_jaccard_similarity::dissimilarity_test<const py_tuple_sketch&, const py_tuple_sketch&>,
        py::arg("actual"), py::arg("expected"), py::arg("threshold"), py::arg("seed")=DEFAULT_SEED,
        "Tests dissimilarity of an actual sketch against an expected sketch. Computes the upper bound of the Jaccard "
        "index J_{UB} of the actual and expected sketches. If J_{UB} <= threshold, then the sketches are considered "
        "to be dissimilar with a confidence of 97.7% and returns True, otherwise False."
    )
  ;
}
