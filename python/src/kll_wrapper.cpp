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

#include "kll_sketch.hpp"

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include <sstream>
#include <vector>

namespace py = pybind11;

namespace datasketches {

// Wrapper class for Numpy compatibility
template <typename T, typename C = std::less<T>, typename S = serde<T>>
class kll_sketches {
  public:
    static const uint32_t DEFAULT_K = kll_sketch<T, C, S>::DEFAULT_K;
    static const uint32_t DEFAULT_D = 1;

    explicit kll_sketches(uint32_t k = DEFAULT_K, uint32_t d = DEFAULT_D);

    // container parameters
    inline uint32_t get_k() const;
    inline uint32_t get_d() const;

    // sketch updates
    void update(const py::array_t<T>& items);

    // sketch queries returning an array of results
    py::array is_empty() const;
    py::array get_n() const;
    py::array is_estimation_mode() const;
    py::array get_min_values() const;
    py::array get_max_values() const;
    py::array get_num_retained() const;
    py::array get_quantiles(const py::array_t<double>& fractions, const py::array_t<int>& isk) const;
    py::array get_ranks(const py::array_t<T>& values, const py::array_t<int>& isk) const;
    py::array get_pmf(const py::array_t<T>& split_points, const py::array_t<int>& isk) const;
    py::array get_cdf(const py::array_t<T>& split_points, const py::array_t<int>& isk) const;

    // human-readable output
    std::string to_string(bool print_levels = false, bool print_items = false) const;

    // binary output/input
    py::list serialize(py::array_t<uint32_t>& isk);
    // note: deserialize() replaces the sketch at the specified
    //       index. Not a static method.
    void deserialize(const py::bytes& sk_bytes, uint32_t idx);

  private:
    std::vector<uint32_t> get_indices(const py::array_t<int>& isk) const;

    const uint32_t k_; // kll sketch k parameter
    const uint32_t d_; // number of dimensions (here: sketches) to hold
    std::vector<kll_sketch<T,C,S>> sketches_;
};

template<typename T, typename C, typename S>
kll_sketches<T,C,S>::kll_sketches(uint32_t k, uint32_t d):
k_(k), 
d_(d)
{
  // check d is valid (k is checked by kll_sketch)
  if (d < 1) {
    throw std::invalid_argument("D must be >= 1: " + std::to_string(d));
  }

  sketches_.reserve(d);
  // spawn the sketches
  for (uint32_t i = 0; i < d; i++) {
    sketches_.emplace_back(k);
  }
}

template<typename T, typename C, typename S>
uint32_t kll_sketches<T,C,S>::get_k() const {
  return k_;
}

template<typename T, typename C, typename S>
uint32_t kll_sketches<T,C,S>::get_d() const {
  return d_;
}

template<typename T, typename C, typename S>
std::vector<uint32_t> kll_sketches<T,C,S>::get_indices(const py::array_t<int>& isk) const {
  std::vector<uint32_t> indices;
  if (isk.size() == 1) {
    auto data = isk.unchecked();
    if (data(0) == -1) {
      indices.reserve(d_);
      for (uint32_t i = 0; i < d_; ++i) {
        indices.push_back(i);
      }
    } else {
      indices.push_back(static_cast<uint32_t>(data(0)));
    }
  } else {
    auto data = isk.unchecked<1>();
    indices.reserve(isk.size());
    for (uint32_t i = 0; i < isk.size(); ++i) {
      const uint32_t idx = static_cast<uint32_t>(data(i));
      if (idx < d_) {
        indices.push_back(idx);
      } else {
        throw std::invalid_argument("request for invalid dimenions >= d ("
                 + std::to_string(d_) +"): "+ std::to_string(idx));
      }
    }
  }
  return indices;
}

// Checks if each sketch is empty or not
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::is_empty() const {
  std::vector<bool> vals(d_);
  for (uint32_t i = 0; i < d_; ++i) {
    vals[i] = sketches_[i].is_empty();
  }

  return py::cast(vals);
}

// Updates each sketch with values
// Currently: all values must be present
// TODO: allow subsets of sketches to be updated
template<typename T, typename C, typename S>
void kll_sketches<T,C,S>::update(const py::array_t<T>& items) {
  if (items.shape(0) != d_) {
    throw std::invalid_argument("input data must have rows with  " + std::to_string(d_)
          + " elements. Found: " + std::to_string(items.shape(0)));
  }
 
  size_t ndim = items.ndim();
  
  if (ndim == 1) {
    // 1D case: single value to update per sketch
    auto data = items.template unchecked<1>();
    for (uint32_t i = 0; i < d_; ++i) {
      sketches_[i].update(data(i));
    }
  }
  else if (ndim == 2) {
    // 2D case: multiple values to update per sketch
    auto data = items.template unchecked<2>();
    if (items.flags() & py::array::f_style) {
      for (uint32_t j = 0; j < d_; ++j) {
        for (uint32_t i = 0; i < items.shape(1); ++i) { 
          sketches_[j].update(data(i,j));
        }
      }
    } else { // py::array::c_style or py::array::forcecast 
      for (uint32_t i = 0; i < items.shape(1); ++i) { 
        for (uint32_t j = 0; j < d_; ++j) {
          sketches_[j].update(data(i,j));
        }
      }
    }
  }
  else {
    throw std::invalid_argument("Update input must be 2 or fewer dimensions : " + std::to_string(ndim));
  }
}

// Number of updates for each sketch
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::get_n() const {
  std::vector<uint64_t> vals(d_);
  for (uint32_t i = 0; i < d_; ++i) {
    vals[i] = sketches_[i].get_n();
  }
  return py::cast(vals);
}

// Number of retained values for each sketch
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::get_num_retained() const {
  std::vector<uint32_t> vals(d_);
  for (uint32_t i = 0; i < d_; ++i) {
    vals[i] = sketches_[i].get_num_retained();
  }
  return py::cast(vals);
}

// Gets the minimum value of each sketch
// TODO: allow subsets of sketches
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::get_min_values() const {
  std::vector<T> vals(d_);
  for (uint32_t i = 0; i < d_; ++i) {
    vals[i] = sketches_[i].get_min_value();
  }
  return py::cast(vals);
}

// Gets the maximum value of each sketch
// TODO: allow subsets of sketches
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::get_max_values() const {
  std::vector<T> vals(d_);
  for (uint32_t i = 0; i < d_; ++i) {
    vals[i] = sketches_[i].get_max_value();
  }
  return py::cast(vals);
}

// Summary of each sketch as one long string
// Users should use .split('\n\n') when calling it to build a list of each 
// sketch's summary
template<typename T, typename C, typename S>
std::string kll_sketches<T,C,S>::to_string(bool print_levels, bool print_items) const {
  std::ostringstream ss;
  for (uint32_t i = 0; i < d_; ++i) {
    // all streams into 1 string, for compatibility with Python's str() behavior
    // users will need to split by \n\n, e.g., str(kll).split('\n\n')
    if (i > 0) ss << "\n";
    ss << sketches_[i].to_string(print_levels, print_items);
  }
  return ss.str();
}

template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::is_estimation_mode() const {
  std::vector<bool> vals(d_);
  for (uint32_t i = 0; i < d_; ++i) {
    vals[i] = sketches_[i].is_estimation_mode();
  }
  return py::cast(vals);
}

// Value of sketch(es) corresponding to some quantile(s)
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::get_quantiles(const py::array_t<double>& fractions, 
                                             const py::array_t<int>& isk) const {
  std::vector<uint32_t> inds = get_indices(isk);
  size_t num_sketches = inds.size();
  size_t num_quantiles = fractions.size();

  std::vector<std::vector<T>> quants(num_sketches, std::vector<T>(num_quantiles));
  for (uint32_t i = 0; i < num_sketches; ++i) {
    auto quant = sketches_[inds[i]].get_quantiles(fractions.data(), num_quantiles);
    for (size_t j = 0; j < num_quantiles; ++j) {
      quants[i][j] = quant[j];
    }
  }

  return py::cast(quants);
}

// Value of sketch(es) corresponding to some rank(s)
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::get_ranks(const py::array_t<T>& values, 
                                         const py::array_t<int>& isk) const {
  std::vector<uint32_t> inds = get_indices(isk);
  size_t num_sketches = inds.size();
  size_t num_ranks = values.size();
  auto vals = values.data();

  std::vector<std::vector<float>> ranks(num_sketches, std::vector<float>(num_ranks));
  for (uint32_t i = 0; i < num_sketches; ++i) {
    for (size_t j = 0; j < num_ranks; ++j) {
      ranks[i][j] = sketches_[inds[i]].get_rank(vals[j]);
    }
  }

  return py::cast(ranks);
}

// PMF(s) of sketch(es)
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::get_pmf(const py::array_t<T>& split_points, 
                                       const py::array_t<int>& isk) const {
  std::vector<uint32_t> inds = get_indices(isk);
  size_t num_sketches = inds.size();
  size_t num_splits = split_points.size();
  
  std::vector<std::vector<T>> pmfs(num_sketches, std::vector<T>(num_splits + 1));
  for (uint32_t i = 0; i < num_sketches; ++i) {
    auto pmf = sketches_[inds[i]].get_PMF(split_points.data(), num_splits);
    for (size_t j = 0; j <= num_splits; ++j) {
      pmfs[i][j] = pmf[j];
    }
  }

  return py::cast(pmfs);
}

// CDF(s) of sketch(es)
template<typename T, typename C, typename S>
py::array kll_sketches<T,C,S>::get_cdf(const py::array_t<T>& split_points, 
                                       const py::array_t<int>& isk) const {
  std::vector<uint32_t> inds = get_indices(isk);
  size_t num_sketches = inds.size();
  size_t num_splits = split_points.size();
  
  std::vector<std::vector<T>> cdfs(num_sketches, std::vector<T>(num_splits + 1));
  for (uint32_t i = 0; i < num_sketches; ++i) {
    auto cdf = sketches_[inds[i]].get_CDF(split_points.data(), num_splits);
    for (size_t j = 0; j <= num_splits; ++j) {
      cdfs[i][j] = cdf[j];
    }
  }

  return py::cast(cdfs);
}

template<typename T, typename C, typename S>
void kll_sketches<T,C,S>::deserialize(const py::bytes& sk_bytes,
                                      uint32_t idx) {
  if (idx >= d_) {
    throw std::invalid_argument("request for invalid dimenions >= d ("
             + std::to_string(d_) +"): "+ std::to_string(idx));
  }
  std::string skStr = sk_bytes; // implicit cast
  // load the sketch into the proper index
  sketches_[idx] = std::move(kll_sketch<T>::deserialize(skStr.c_str(), skStr.length()));
}

template<typename T, typename C, typename S>
py::list kll_sketches<T,C,S>::serialize(py::array_t<uint32_t>& isk) {
  std::vector<uint32_t> inds = get_indices(isk);
  const size_t num_sketches = inds.size();

  py::list list(num_sketches);
  for (uint32_t i = 0; i < num_sketches; ++i) {
    auto serResult = sketches_[inds[i]].serialize();
    list[i] = py::bytes((char*)serResult.data(), serResult.size());
  }

  return list;
}


namespace python {

template<typename T>
kll_sketch<T> kll_sketch_deserialize(py::bytes skBytes) {
  std::string skStr = skBytes; // implicit cast  
  return kll_sketch<T>::deserialize(skStr.c_str(), skStr.length());
}

template<typename T>
py::object kll_sketch_serialize(const kll_sketch<T>& sk) {
  auto serResult = sk.serialize();
  return py::bytes((char*)serResult.data(), serResult.size());
}

// maybe possible to disambiguate the static vs method rank error calls, but
// this is easier for now
template<typename T>
double kll_sketch_generic_normalized_rank_error(uint16_t k, bool pmf) {
  return kll_sketch<T>::get_normalized_rank_error(k, pmf);
}

template<typename T>
py::list kll_sketch_get_quantiles(const kll_sketch<T>& sk,
                                 std::vector<double>& fractions) {
  size_t nQuantiles = fractions.size();
  auto result = sk.get_quantiles(&fractions[0], nQuantiles);

  // returning as std::vector<> would copy values to a list anyway
  py::list list(nQuantiles);
  for (size_t i = 0; i < nQuantiles; ++i) {
      list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list kll_sketch_get_pmf(const kll_sketch<T>& sk,
                            std::vector<T>& split_points) {
  size_t nPoints = split_points.size();
  auto result = sk.get_PMF(&split_points[0], nPoints);

  py::list list(nPoints + 1);
  for (size_t i = 0; i <= nPoints; ++i) {
    list[i] = result[i];
  }

  return list;
}

template<typename T>
py::list kll_sketch_get_cdf(const kll_sketch<T>& sk,
                            std::vector<T>& split_points) {
  size_t nPoints = split_points.size();
  auto result = sk.get_CDF(&split_points[0], nPoints);

  py::list list(nPoints + 1);
  for (size_t i = 0; i <= nPoints; ++i) {
    list[i] = result[i];
  }

  return list;
}


}
}

namespace dspy = datasketches::python;

template<typename T>
void bind_kll_sketch(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<kll_sketch<T>>(m, name)
    .def(py::init<uint16_t>(), py::arg("k"))
    .def(py::init<const kll_sketch<T>&>())
    .def("update", (void (kll_sketch<T>::*)(const T&)) &kll_sketch<T>::update, py::arg("item"))
    .def("merge", (void (kll_sketch<T>::*)(const kll_sketch<T>&)) &kll_sketch<T>::merge, py::arg("sketch"))
    .def("__str__", &kll_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false)
    .def("to_string", &kll_sketch<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false)
    .def("is_empty", &kll_sketch<T>::is_empty)
    .def("get_n", &kll_sketch<T>::get_n)
    .def("get_num_retained", &kll_sketch<T>::get_num_retained)
    .def("is_estimation_mode", &kll_sketch<T>::is_estimation_mode)
    .def("get_min_value", &kll_sketch<T>::get_min_value)
    .def("get_max_value", &kll_sketch<T>::get_max_value)
    .def("get_quantile", &kll_sketch<T>::get_quantile, py::arg("fraction"))
    .def("get_quantiles", &dspy::kll_sketch_get_quantiles<T>, py::arg("fractions"))
    .def("get_rank", &kll_sketch<T>::get_rank, py::arg("value"))
    .def("get_pmf", &dspy::kll_sketch_get_pmf<T>, py::arg("split_points"))
    .def("get_cdf", &dspy::kll_sketch_get_cdf<T>, py::arg("split_points"))
    .def("normalized_rank_error", (double (kll_sketch<T>::*)(bool) const) &kll_sketch<T>::get_normalized_rank_error,
         py::arg("as_pmf"))
    .def_static("get_normalized_rank_error", &dspy::kll_sketch_generic_normalized_rank_error<T>,
         py::arg("k"), py::arg("as_pmf"))
    .def("serialize", &dspy::kll_sketch_serialize<T>)
    .def_static("deserialize", &dspy::kll_sketch_deserialize<T>)
    ;
}

template<typename T>
void bind_kll_sketches(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<kll_sketches<T>>(m, name)
    .def(py::init<uint32_t, uint32_t>(), py::arg("k")=kll_sketches<T>::DEFAULT_K, 
                                         py::arg("d")=kll_sketches<T>::DEFAULT_D)
    .def(py::init<const kll_sketches<T>&>())
    // allow user to retrieve k or d, in case it's instantiated w/ defaults
    .def("get_k", &kll_sketches<T>::get_k,
         "Returns the value of `k` of the sketch(es)")
    .def("get_d", &kll_sketches<T>::get_d,
         "Returns the number of sketches")
    .def("update", &kll_sketches<T>::update, py::arg("items"), 
         "Updates the sketch(es) with value(s).  Must be a 1D array of size equal to the number of sketches.  Can also be 2D array of shape (n_updates, n_sketches).  If a sketch does not have a value to update, use np.nan")
    .def("__str__", &kll_sketches<T>::to_string, py::arg("print_levels")=false, py::arg("print_items")=false,
         "Produces a string summary of all sketches. Users should split the returned string by '\n\n'")
    .def("to_string", &kll_sketches<T>::to_string, py::arg("print_levels")=false,
                                                   py::arg("print_items")=false,
         "Produces a string summary of all sketches. Users should split the returned string by '\n\n'")
    .def("is_empty", &kll_sketches<T>::is_empty,
         "Returns whether the sketch(es) is(are) empty of not")
    .def("get_n", &kll_sketches<T>::get_n, 
         "Returns the number of values seen by the sketch(es)")
    .def("get_num_retained", &kll_sketches<T>::get_num_retained, 
         "Returns the number of values retained by the sketch(es)")
    .def("is_estimation_mode", &kll_sketches<T>::is_estimation_mode, 
         "Returns whether the sketch(es) is(are) in estimation mode")
    .def("get_min_values", &kll_sketches<T>::get_min_values,
         "Returns the minimum value(s) of the sketch(es)")
    .def("get_max_values", &kll_sketches<T>::get_max_values,
         "Returns the maximum value(s) of the sketch(es)")
    .def("get_quantiles", &kll_sketches<T>::get_quantiles, py::arg("fractions"), 
                                                           py::arg("isk")=-1, 
         "Returns the value(s) associated with the specified quantile(s) for the specified sketch(es). `fractions` can be a float between 0 and 1 (inclusive), or a list/array of values. `isk` specifies which sketch(es) to return the value(s) for (default: all sketches)")
    .def("get_ranks", &kll_sketches<T>::get_ranks, py::arg("values"), 
                                                   py::arg("isk")=-1, 
         "Returns the value(s) associated with the specified ranks(s) for the specified sketch(es). `values` can be an int between 0 and the number of values retained, or a list/array of values. `isk` specifies which sketch(es) to return the value(s) for (default: all sketches)")
    .def("get_pmf", &kll_sketches<T>::get_pmf, py::arg("split_points"), py::arg("isk")=-1, 
         "Returns the probability mass function (PMF) at `split_points` of the specified sketch(es).  `split_points` should be a list/array of floats between 0 and 1 (inclusive). `isk` specifies which sketch(es) to return the PMF for (default: all sketches)")
    .def("get_cdf", &kll_sketches<T>::get_cdf, py::arg("split_points"), py::arg("isk")=-1, 
         "Returns the cumulative distribution function (CDF) at `split_points` of the specified sketch(es).  `split_points` should be a list/array of floats between 0 and 1 (inclusive). `isk` specifies which sketch(es) to return the CDF for (default: all sketches)")
    .def_static("get_normalized_rank_error", &dspy::kll_sketch_generic_normalized_rank_error<T>,
         py::arg("k"), py::arg("as_pmf"), "Returns the normalized rank error")
    .def("serialize", &kll_sketches<T>::serialize, py::arg("isk")=-1, 
         "Serializes the specified sketch(es). `isk` can be an int or a list/array of ints (default: all sketches)")
    .def("deserialize", &kll_sketches<T>::deserialize, py::arg("skBytes"), py::arg("isk"), 
         "Deserializes the specified sketch.  `isk` must be an int.")
    // FINDME The following have not yet been implemented:
    //.def("merge", (void (kll_sketch<T>::*)(const kll_sketch<T>&)) &kll_sketch<T>::merge, py::arg("sketch"))
    ;
}


void init_kll(py::module &m) {
  bind_kll_sketch<int>(m, "kll_ints_sketch");
  bind_kll_sketch<float>(m, "kll_floats_sketch");
  bind_kll_sketches<int>(m, "kll_intarray_sketches");
  bind_kll_sketches<float>(m, "kll_floatarray_sketches");
}
