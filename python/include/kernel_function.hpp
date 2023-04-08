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

//#include <memory>
#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>

#ifndef _KERNEL_FUNCTION_HPP_
#define _KERNEL_FUNCTION_HPP_

namespace py = pybind11;

namespace datasketches {

/**
 * @brief kernel_function provides the underlying base class from
 *        which native Python kernels ultimately inherit. The actual
 *        kernels implement KernelFunction, as shown in KernelFunction.py
 */
struct kernel_function {
  virtual double operator()(py::array_t<double>& a, const py::array_t<double>& b) const = 0;
  virtual ~kernel_function() = default;
};

/**
 * @brief KernelFunction provides the "trampoline" class for pybind11
 *        that allows for a native Python implementation of kernel
 *        functions.
 */
struct KernelFunction : public kernel_function {
  using kernel_function::kernel_function;

  /**
   * @brief Evaluates K(a,b), the kernel function for the given points a and b
   * @param a the first vector
   * @param b the second vector
   * @return The function value K(a,b)
   */
  double operator()(py::array_t<double>& a, const py::array_t<double>& b) const override {
    PYBIND11_OVERRIDE_PURE_NAME(
      double,          // Return type
      kernel_function, // Parent class
      "__call__",      // Name of function in python
      operator(),      // Name of function in C++
      a, b             // Arguemnts
    );
  }
};

/* The kernel_function_holder provides a concrete class that dispatches calls
 * from the sketch to the kernel_function. This class is needed to provide a
 * concrete object to produce a compiled library, but library users should
 * never need to use this directly.
 */
struct kernel_function_holder {
  explicit kernel_function_holder(std::shared_ptr<kernel_function> kernel) : _kernel(kernel) {}
  kernel_function_holder(const kernel_function_holder& other) : _kernel(other._kernel) {}
  kernel_function_holder(kernel_function_holder&& other) : _kernel(std::move(other._kernel)) {}
  kernel_function_holder& operator=(const kernel_function_holder& other) { _kernel = other._kernel; return *this; }
  kernel_function_holder& operator=(kernel_function_holder&& other) { std::swap(_kernel, other._kernel); return *this; }

  double operator()(const std::vector<double>& a, const py::array_t<double>& b) const {
    py::array_t<double> a_arr(a.size(), a.data(), dummy_array_owner);
    return _kernel->operator()(a_arr, b);
  }

  double operator()(const std::vector<double>& a, const std::vector<double>& b) const {
    py::array_t<double> a_arr(a.size(), a.data(), dummy_array_owner);
    py::array_t<double> b_arr(b.size(), b.data(), dummy_array_owner);
    return _kernel->operator()(a_arr, b_arr);
  }

  private:
    // a dummy object to "own" arrays when translating from std::vector to avoid a copy:
    // https://github.com/pybind/pybind11/issues/323#issuecomment-575717041
    py::str dummy_array_owner;
    std::shared_ptr<kernel_function> _kernel;
};

}

#endif // _KERNEL_FUNCTION_HPP_