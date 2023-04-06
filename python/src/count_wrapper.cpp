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

#include "count_min.hpp"
#include "common_defs.hpp"

namespace py = pybind11;

template<typename W>
void bind_count_min_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<count_min_sketch<W>>(m, name)
    .def(py::init<uint8_t, uint32_t, uint64_t>(), py::arg("num_hashes"), py::arg("num_buckets"), py::arg("seed")=DEFAULT_SEED)
    .def(py::init<const count_min_sketch<W>&>())
    .def_static("suggest_num_buckets", &count_min_sketch<W>::suggest_num_buckets, py::arg("relative_error"),
                "Suggests the number of buckets needed to achieve an accuracy within the provided "
                "relative_error. For example, when relative_error = 0.05, the returned frequency estimates "
                "satisfy the 'relative_error' guarantee that never overestimates the weights but may "
                "underestimate the weights by 5% of the total weight in the sketch. "
                "Returns the number of hash buckets at every level of the sketch required in order to obtain "
                "the specified relative error.")
    .def_static("suggest_num_hashes", &count_min_sketch<W>::suggest_num_hashes, py::arg("confidence"),
                "Suggests the number of hashes needed to achieve the provided confidence. For example, "
                "with 95% confidence, frequency estimates satisfy the 'relative_error' guarantee. "
                "Returns the number of hash functions that are required in order to achieve the specified "
                "confidence of the sketch. confidence = 1 - delta, with delta denoting the sketch failure probability.")
    .def("__str__", &count_min_sketch<W>::to_string,
         "Produces a string summary of the sketch")
    .def("to_string", &count_min_sketch<W>::to_string,
         "Produces a string summary of the sketch")
    .def("is_empty", &count_min_sketch<W>::is_empty,
         "Returns True if the sketch has seen no items, otherwise False")
    .def("get_num_hashes", &count_min_sketch<W>::get_num_hashes,
         "Returns the configured number of hashes for the sketch")
    .def("get_num_buckets", &count_min_sketch<W>::get_num_buckets,
         "Returns the configured number of buckets for the sketch")
    .def("get_seed", &count_min_sketch<W>::get_seed,
         "Returns the base hash seed for the sketch")
    .def("get_relative_error", &count_min_sketch<W>::get_relative_error,
         "Returns the maximum permissible error for any frequency estimate query")
    .def("get_total_weight", &count_min_sketch<W>::get_total_weight,
         "Returns the total weight currently inserted into the stream")
    .def("update", static_cast<void (count_min_sketch<W>::*)(int64_t, W)>(&count_min_sketch<W>::update), py::arg("item"), py::arg("weight")=1.0,
         "Updates the sketch with the given 64-bit integer value")
    .def("update", static_cast<void (count_min_sketch<W>::*)(const std::string&, W)>(&count_min_sketch<W>::update), py::arg("item"), py::arg("weight")=1.0,
         "Updates the sketch with the given string")
    .def("get_estimate", static_cast<W (count_min_sketch<W>::*)(int64_t) const>(&count_min_sketch<W>::get_estimate), py::arg("item"),
         "Returns an estimate of the frequency of the provided 64-bit integer value")
    .def("get_estimate", static_cast<W (count_min_sketch<W>::*)(const std::string&) const>(&count_min_sketch<W>::get_estimate), py::arg("item"),
         "Returns an estimate of the frequency of the provided string")
    .def("get_upper_bound", static_cast<W (count_min_sketch<W>::*)(int64_t) const>(&count_min_sketch<W>::get_upper_bound), py::arg("item"),
         "Returns an upper bound on the estimate for the given 64-bit integer value")
    .def("get_upper_bound", static_cast<W (count_min_sketch<W>::*)(const std::string&) const>(&count_min_sketch<W>::get_upper_bound), py::arg("item"),
         "Returns an upper bound on the estimate for the provided string")
    .def("get_lower_bound", static_cast<W (count_min_sketch<W>::*)(int64_t) const>(&count_min_sketch<W>::get_lower_bound), py::arg("item"),
         "Returns an lower bound on the estimate for the given 64-bit integer value")
    .def("get_lower_bound", static_cast<W (count_min_sketch<W>::*)(const std::string&) const>(&count_min_sketch<W>::get_lower_bound), py::arg("item"),
         "Returns an lower bound on the estimate for the provided string")
    .def("merge", &count_min_sketch<W>::merge, py::arg("other"),
         "Merges the provided other sketch into this one")
    .def("get_serialized_size_bytes", &count_min_sketch<W>::get_serialized_size_bytes,
         "Returns the size in bytes of the serialized image of the sketch")
    .def(
        "serialize",
        [](const count_min_sketch<W>& sk) {
          auto bytes = sk.serialize();
          return py::bytes(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        },
        "Serializes the sketch into a bytes object"
    )
    .def_static(
        "deserialize",
        [](const std::string& bytes) { return count_min_sketch<W>::deserialize(bytes.data(), bytes.size()); },
        py::arg("bytes"),
        "Reads a bytes object and returns the corresponding count_min_sketch"
    );
}

void init_count_min(py::module &m) {
  bind_count_min_sketch<double>(m, "count_min_sketch");
}

