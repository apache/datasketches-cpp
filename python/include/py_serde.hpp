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
#include <pybind11/functional.h>
#include <sstream>

#ifndef _PY_SERDE_HPP_
#define _PY_SERDE_HPP_

namespace py = pybind11;

namespace datasketches {

struct py_object_serde {
  // only raw bytes serialization, no stream
  virtual int64_t get_size(const py::object& item) const = 0;
  virtual py::bytes to_bytes(const py::object& item) const = 0;
  virtual py::tuple from_bytes(py::bytes& bytes, size_t offset) const = 0;

  virtual ~py_object_serde() = default;

  size_t size_of_item(const py::object& item) const;
  size_t serialize(void* ptr, size_t capacity, const py::object* items, unsigned num) const;
  size_t deserialize(const void* ptr, size_t capacity, py::object* items, unsigned num) const;
};

struct PyObjectSerde : public py_object_serde {
  using py_object_serde::py_object_serde;

  // trampoline (need one for each virtual function)
  int64_t get_size(const py::object& item) const override {
    PYBIND11_OVERRIDE_PURE(
      int64_t,         // Return type
      py_object_serde, // Parent class
      get_size,        // Name of function in C++ (must match Python name)
      item             // Argument(s)
    );
  }

  py::bytes to_bytes(const py::object& item) const override {
    PYBIND11_OVERRIDE_PURE(
      py::bytes,       // Return type
      py_object_serde, // Parent class
      to_bytes,        // Name of function in C++ (must match Python name)
      item             // Argument(s)
    );
  }

  py::tuple from_bytes(py::bytes& bytes, size_t offset) const override {
    PYBIND11_OVERRIDE_PURE(
      py::tuple,        // Return type
      py_object_serde,  // Parent class
      from_bytes,       // Name of function in C++ (must match Python name)
      bytes, offset     // Argument(s)
    );
  }
};

}

#endif // _PY_SERDE_HPP_
