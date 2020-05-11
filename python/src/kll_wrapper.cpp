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

namespace py = pybind11;

namespace datasketches {

// Wrapper class for Numpy compatibility
template <typename T, typename C = std::less<T>, typename S = serde<T>, typename A = std::allocator<T>>
class kll_sketches {
  public:
    uint16_t k, d;
    std::vector <kll_sketch<T, C, S, A>> sketches;
    static const uint16_t DEFAULT_K = kll_sketch<T, C, S, A>::DEFAULT_K;
    static const uint16_t DEFAULT_D = 1;

    explicit kll_sketches(uint16_t k = DEFAULT_K, uint16_t d = DEFAULT_D);
};

template<typename T, typename C, typename S, typename A>
kll_sketches<T, C, S, A>::kll_sketches(uint16_t kval, uint16_t dval):
k(kval), 
d(dval)
{
  // check d is valid (k is checked by kll_sketch)
  if (d < 1) {
    throw std::invalid_argument("D must be >= 1: " + std::to_string(d));
  }

  // spawn the sketches
  for (uint16_t i; i < d; i++) {
    sketches.emplace_back(k);
  }
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

template<typename T>
std::string kll_sketch_to_string(const kll_sketch<T>& sk) {
  std::ostringstream ss;
  sk.to_stream(ss);
  return ss.str();
}

// Updates: allow parallel sketches via Numpy
// Helper functions to allow subsets of sketches to be selected
template<typename T>
uint16_t kll_sketches_get_num_inds(const kll_sketches<T>& sks, 
                                   py::array_t<int>& isk) {
  uint16_t nSketches;

  auto ibuf = isk.request();
  int *iptr = (int *) ibuf.ptr;
  if ((ibuf.size == 1) && (iptr[0] == -1)) {
    nSketches = sks.d;
  }
  else {
    nSketches = (uint16_t)ibuf.size;
  }

  return nSketches;
}

std::vector<uint16_t> kll_sketches_get_inds(py::array_t<int>& isk, 
                                            uint16_t nSketches) {
  std::vector<uint16_t> inds(nSketches);

  auto ibuf = isk.request();
  int *iptr = (int *) ibuf.ptr;
  if ((ibuf.size == 1) && (iptr[0] == -1)) {
    for (uint16_t n = 0; n < nSketches; ++n) {
      inds[n] = n;
    }
  }
  else {
    for (uint16_t n = 0; n < nSketches; ++n) {
      inds[n] = iptr[n];
    }
  }

  return inds;
}

// Checks if each sketch is empty or not
template<typename T>
py::array kll_sketches_is_empty(const kll_sketches<T>& sks) {
  uint16_t nSketches = sks.d;

  std::vector<bool> vals(nSketches);
  for (uint16_t i = 0; i < nSketches; ++i) {
    vals[i] = sks.sketches[i].is_empty();
  }

  py::array result = py::cast(vals);
  return result;
}

// Updates each sketch with values
// Currently: all values must be present
// TODO: allow subsets of sketches to be updated
template<typename T, typename A>
void kll_sketches_update(kll_sketches<T>& sks, py::array_t<A>& items) {
  auto buf = items.request();
  double *ptr = (double *) buf.ptr;

  size_t ndim = buf.ndim;

  if (ndim == 1) {
    // 1D case: single value to update per sketch
    for (int i = 0; i < buf.size; ++i) {
      sks.sketches[i].update(ptr[i]);
    }
  }
  else {
    // 2D case: multiple values to update per sketch
    for (int i = 0; i < buf.shape[0]; ++i) {
      for (int j = 0; j < buf.shape[1]; ++j) {
        sks.sketches[j].update(ptr[i*buf.shape[1] + j]);
      }
    }
  }
}

// Number of updates for each sketch
template<typename T>
py::array kll_sketches_get_n(const kll_sketches<T>& sks) {
  uint16_t nSketches = sks.d;

  std::vector<uint32_t> vals(nSketches);
  for (uint16_t i = 0; i < nSketches; ++i) {
    vals[i] = sks.sketches[i].get_n();
  }

  py::array result = py::cast(vals);
  return result;
}

// Number of retained values for each sketch
template<typename T>
py::array kll_sketches_get_num_retained(const kll_sketches<T>& sks) {
  uint16_t nSketches = sks.d;

  std::vector<uint32_t> vals(nSketches);
  for (uint16_t i = 0; i < nSketches; ++i) {
    vals[i] = sks.sketches[i].get_num_retained();
  }

  py::array result = py::cast(vals);
  return result;
}

// Gets the minimum value of each sketch
// TODO: allow subsets of sketches
template<typename T>
py::array kll_sketches_get_min_values(const kll_sketches<T>& sks) {
  uint16_t nSketches = sks.d;

  std::vector<T> vals(nSketches);
  for (uint16_t i = 0; i < nSketches; ++i) {
    vals[i] = sks.sketches[i].get_min_value();
  }

  py::array result = py::cast(vals);
  return result;
}

// Gets the maximum value of each sketch
// TODO: allow subsets of sketches
template<typename T>
py::array kll_sketches_get_max_values(const kll_sketches<T>& sks) {
  uint16_t nSketches = sks.d;

  std::vector<T> vals(nSketches);
  for (uint16_t i = 0; i < nSketches; ++i) {
    vals[i] = sks.sketches[i].get_max_value();
  }

  py::array result = py::cast(vals);
  return result;
}

// Summary of each sketch as one long string
// Users should use .split('\n\n') when calling it to build a list of each 
// sketch's summary
template<typename T>
std::string kll_sketches_to_strings(const kll_sketches<T>& sks) {
  std::ostringstream ss;
  std::string rs;
  uint16_t nSketches = sks.d;

  for (uint16_t i = 0; i < nSketches; ++i) {
    // all streams into 1 string, for compatibility with Python's str() behavior
    sks.sketches[i].to_stream(ss);
    // users will need to split by \n\n, e.g., str(kll).split('\n\n')
    ss << "\n";
  }

  // remove the last 2 \n characters so that (1) there isn't an extra blank 
  // entry when user splits it and (2) same format for all sketch summaries
  rs = ss.str();
  rs.erase(rs.size() - 2, 2);
  return rs;
}

template<typename T>
py::array kll_sketches_is_estimation_mode(const kll_sketches<T>& sks) {
  uint16_t nSketches = sks.d;

  std::vector<bool> vals(nSketches);
  for (uint16_t i = 0; i < nSketches; ++i) {
    vals[i] = sks.sketches[i].is_estimation_mode();
  }

  py::array result = py::cast(vals);
  return result;
}

// Value of sketch(es) corresponding to some quantile(s)
template<typename T, typename A>
py::array kll_sketches_get_quantiles(const kll_sketches<T>& sks, 
                                     py::array_t<A>& fractions, 
                                     py::array_t<int>& isk) {
  uint16_t nSketches = kll_sketches_get_num_inds(sks, isk);
  std::vector<uint16_t> inds = kll_sketches_get_inds(isk, nSketches);

  auto buf = fractions.request();
  int nQuantiles = buf.size;
  double *ptr = (double *) buf.ptr;

  std::vector<std::vector<T>> quants(nSketches, std::vector<T> (nQuantiles));
  for (uint16_t i = 0; i < nSketches; ++i) {
    auto quant = sks.sketches[inds[i]].get_quantiles(&ptr[0], nQuantiles);
    for (int j = 0; j < nQuantiles; ++j) {
      quants[i][j] = quant[j];
    }
  }

  py::array result = py::cast(quants);
  return result;
}

// Value of sketch(es) corresponding to some rank(s)
template<typename T, typename A>
py::array kll_sketches_get_ranks(const kll_sketches<T>& sks, 
                                 py::array_t<A>& values, 
                                 py::array_t<int>& isk) {
  uint16_t nSketches = kll_sketches_get_num_inds(sks, isk);
  std::vector<uint16_t> inds = kll_sketches_get_inds(isk, nSketches);

  auto buf = values.request();
  int nRanks = buf.size;
  double *ptr = (double *) buf.ptr;

  std::vector<std::vector<T>> ranks(nSketches, std::vector<T> (nRanks));
  for (uint16_t i = 0; i < nSketches; ++i) {
    for (int j = 0; j < nRanks; ++j) {
      ranks[i][j] = sks.sketches[inds[i]].get_rank(ptr[j]);
    }
  }

  py::array result = py::cast(ranks);
  return result;
}

// PMF(s) of sketch(es)
template<typename T>
py::array kll_sketches_get_pmf(const kll_sketches<T>& sks, 
                               std::vector<T>& split_points, 
                               py::array_t<int>& isk) {
  uint16_t nSketches = kll_sketches_get_num_inds(sks, isk);
  std::vector<uint16_t> inds = kll_sketches_get_inds(isk, nSketches);

  size_t nPoints = split_points.size();

  std::vector<std::vector<T>> pmfs(nSketches, std::vector<T> (nPoints + 1));

  for (uint16_t i = 0; i < nSketches; ++i) {
    auto pmf = sks.sketches[inds[i]].get_PMF(&split_points[0], nPoints);
    for (size_t j = 0; j <= nPoints; ++j) {
      pmfs[i][j] = pmf[j];
    }
  }

  py::array result = py::cast(pmfs);
  return result;
}

// CDF(s) of sketch(es)
template<typename T>
py::array kll_sketches_get_cdf(const kll_sketches<T>& sks, 
                               std::vector<T>& split_points, 
                               py::array_t<int>& isk) {
  uint16_t nSketches = kll_sketches_get_num_inds(sks, isk);
  std::vector<uint16_t> inds = kll_sketches_get_inds(isk, nSketches);

  size_t nPoints = split_points.size();

  std::vector<std::vector<T>> cdfs(nSketches, std::vector<T> (nPoints + 1));

  for (uint16_t i = 0; i < nSketches; ++i) {
    auto cdf = sks.sketches[inds[0]].get_CDF(&split_points[0], nPoints);
    for (size_t j = 0; j <= nPoints; ++j) {
      cdfs[i][j] = cdf[j];
    }
  }

  py::array result = py::cast(cdfs);
  return result;
}

template<typename T>
void kll_sketches_deserialize(kll_sketches<T>& sks, py::bytes skBytes, uint16_t isk) {
  std::string skStr = skBytes; // implicit cast
  // load the sketch into the proper index
  sks.sketches[isk] = kll_sketch<T>::deserialize(skStr.c_str(), skStr.length());
}

template<typename T>
py::list kll_sketches_serialize(const kll_sketches<T>& sks, 
                                  py::array_t<int>& isk) {
  uint16_t nSketches = kll_sketches_get_num_inds(sks, isk);
  std::vector<uint16_t> inds = kll_sketches_get_inds(isk, nSketches);

  py::list list(nSketches);
  for (uint16_t i = 0; i < nSketches; ++i) {
    auto serResult = sks.sketches[inds[i]].serialize();
    list[i] = py::bytes((char*)serResult.data(), serResult.size());
  }

  return list;
}

// Helper functions to return value of k or d, in case user creates the object 
// with defaults
template<typename T>
uint16_t kll_sketches_get_k(const kll_sketches<T>& sks) {
  return sks.k;
}

template<typename T>
uint16_t kll_sketches_get_d(const kll_sketches<T>& sks) {
  return sks.d;
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
    .def("__str__", &dspy::kll_sketch_to_string<T>)
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

template<typename T, typename A>
void bind_kll_sketches(py::module &m, const char* name) {
  using namespace datasketches;

  py::class_<kll_sketches<T>>(m, name)
    .def(py::init<uint16_t, uint16_t>(), py::arg("k")=kll_sketches<T>::DEFAULT_K, 
                                         py::arg("d")=kll_sketches<T>::DEFAULT_D)
    .def(py::init<const kll_sketches<T>&>())
    // allow user to retrieve k or d, in case it's instantiated w/ defaults
    .def("get_k", &dspy::kll_sketches_get_k<T>)
    .def("get_d", &dspy::kll_sketches_get_d<T>)
    .def("update", &dspy::kll_sketches_update<T, A>, py::arg("items"))
    .def("__str__", &dspy::kll_sketches_to_strings<T>)
    .def("is_empty", &dspy::kll_sketches_is_empty<T>)
    .def("get_n", &dspy::kll_sketches_get_n<T>)
    .def("get_num_retained", &dspy::kll_sketches_get_num_retained<T>)
    .def("is_estimation_mode", &dspy::kll_sketches_is_estimation_mode<T>)
    .def("get_min_values", &dspy::kll_sketches_get_min_values<T>)
    .def("get_max_values", &dspy::kll_sketches_get_max_values<T>)
    .def("get_quantiles", &dspy::kll_sketches_get_quantiles<T, A>, py::arg("fractions"), 
                                                                   py::arg("isk")=-1)
    .def("get_ranks", &dspy::kll_sketches_get_ranks<T, A>, py::arg("values"), 
                                                           py::arg("isk")=-1)
    .def("get_pmf", &dspy::kll_sketches_get_pmf<T>, py::arg("split_points"), py::arg("isk")=-1)
    .def("get_cdf", &dspy::kll_sketches_get_cdf<T>, py::arg("split_points"), py::arg("isk")=-1)
    .def_static("get_normalized_rank_error", &dspy::kll_sketch_generic_normalized_rank_error<T>,
         py::arg("k"), py::arg("as_pmf"))
    .def("serialize", &dspy::kll_sketches_serialize<T>, py::arg("isk")=-1)
    .def("deserialize", &dspy::kll_sketches_deserialize<T>, py::arg("skBytes"), py::arg("isk"))
    // FINDME The following have not yet been implemented:
    //.def("merge", (void (kll_sketch<T>::*)(const kll_sketch<T>&)) &kll_sketch<T>::merge, py::arg("sketch"))
    ;
}


void init_kll(py::module &m) {
  bind_kll_sketch<int>(m, "kll_ints_sketch");
  bind_kll_sketch<float>(m, "kll_floats_sketch");
  bind_kll_sketches<int, int>(m, "kll_intarray_sketches");
  bind_kll_sketches<float, double>(m, "kll_floatarray_sketches");
}
