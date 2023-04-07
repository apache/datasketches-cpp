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

namespace py = pybind11;

// sketches
void init_hll(py::module& m);
void init_kll(py::module& m);
void init_fi(py::module& m);
void init_cpc(py::module& m);
void init_theta(py::module& m);
void init_tuple(py::module& m);
void init_vo(py::module& m);
void init_req(py::module& m);
void init_quantiles(py::module& m);
void init_density(py::module& m);
void init_count_min(py::module& m);
void init_vector_of_kll(py::module& m);

// supporting objects
void init_kolmogorov_smirnov(py::module& m);
void init_serde(py::module& m);

PYBIND11_MODULE(_datasketches, m) {
  init_hll(m);
  init_kll(m);
  init_fi(m);
  init_cpc(m);
  init_theta(m);
  init_tuple(m);
  init_vo(m);
  init_req(m);
  init_quantiles(m);
  init_density(m);
  init_count_min(m);
  init_vector_of_kll(m);

  init_kolmogorov_smirnov(m);
  init_serde(m);
}
