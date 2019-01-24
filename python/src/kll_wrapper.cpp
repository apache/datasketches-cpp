/*
 * Copyright 2019, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "kll/include/kll_sketch.hpp"
#include <boost/python.hpp>

namespace bpy = boost::python;

namespace datasketches {
namespace python {

template<typename T>
kll_sketch<T>* KllSketch_deserialize(bpy::object obj) {
  PyObject* skBytes = obj.ptr();
  if (!PyBytes_Check(skBytes)) {
    PyErr_SetString(PyExc_TypeError, "Attmpted to deserialize non-bytes object");
    bpy::throw_error_already_set();
    return nullptr;
  }
  
  size_t len = PyBytes_GET_SIZE(skBytes);
  char* sketchImg = PyBytes_AS_STRING(skBytes);
  auto sk = kll_sketch<T>::deserialize(sketchImg, len);
  return sk.release();
}

template<typename T>
bpy::object KllSketch_serialize(const kll_sketch<T>& sk) {
  auto serResult = sk.serialize();
  PyObject* sketchBytes = PyBytes_FromStringAndSize((char*)serResult.first.get(), serResult.second);
  return bpy::object{bpy::handle<>(sketchBytes)};
}

template<typename T>
double KllSketch_sketchNormalizedRankError(const kll_sketch<T>& sk,
                                           bool pmf) {
  return sk.get_normalized_rank_error(pmf);
}

template<typename T>
double KllSketch_generalNormalizedRankError(uint16_t k, bool pmf) {
  return kll_sketch<T>::get_normalized_rank_error(k, pmf);
}

template<typename T>
bpy::list KllSketch_getQuantiles(const kll_sketch<T>& sk,
                                 bpy::list& fractions) {
  size_t nQuantiles = len(fractions);
  double* frac = new double[nQuantiles];
  for (int i = 0; i < nQuantiles; ++i) {
    frac[i] = bpy::extract<double>(fractions[i]);
  }
  std::unique_ptr<T[]> result = sk.get_quantiles(frac, nQuantiles);

  PyObject* list = PyList_New(nQuantiles);
  for (int i = 0; i < nQuantiles; ++i) {
    if (std::is_same<T, int>::value)        
      PyList_SET_ITEM(list, i, PyLong_FromLong(result[i]));
    else if (std::is_same<T, float>::value)
      PyList_SET_ITEM(list, i, PyFloat_FromDouble(result[i]));
  }

  delete [] frac;
  return bpy::list{bpy::handle<>(list)};
}

template<typename T>
bpy::list KllSketch_getPMF(const kll_sketch<T>& sk,
                           bpy::list& split_points) {
  size_t nPoints = len(split_points);
  T* splitPoints = new T[nPoints];
  for (int i = 0; i < nPoints; ++i) {
    splitPoints[i] = bpy::extract<T>(split_points[i]);
  }
  std::unique_ptr<double[]> result = sk.get_PMF(splitPoints, nPoints);

  PyObject* pmf = PyList_New(nPoints);
  for (int i = 0; i < nPoints; ++i) {
    PyList_SET_ITEM(pmf, i, PyFloat_FromDouble(result[i]));
  }

  delete [] splitPoints;
  return bpy::list{bpy::handle<>(pmf)};
}

template<typename T>
bpy::list KllSketch_getCDF(const kll_sketch<T>& sk,
                           bpy::list& split_points) {
  size_t nPoints = len(split_points);
  T* splitPoints = new T[nPoints];
  for (int i = 0; i < nPoints; ++i) {
    splitPoints[i] = bpy::extract<T>(split_points[i]);
  }
  std::unique_ptr<double[]> result = sk.get_CDF(splitPoints, nPoints);

  PyObject* cdf = PyList_New(nPoints);
  for (int i = 0; i < nPoints; ++i) {
    PyList_SET_ITEM(cdf, i, PyFloat_FromDouble(result[i]));
  }

  delete [] splitPoints;
  return bpy::list{bpy::handle<>(cdf)};
}

template<typename T>
uint32_t KllSketch_getSerializedSizeBytes(const kll_sketch<T>& sk) {
  return sk.get_serialized_size_bytes();
}

template<typename T>
std::string KllSketch_toString(const kll_sketch<T>& sk) {
  std::ostringstream ss;
  ss << sk;
  return ss.str();
}

}
}

namespace dspy = datasketches::python;

template<typename T>
void bind_kll_sketch(const char* name)
{
  using namespace datasketches;

  bpy::class_<kll_sketch<T>, boost::noncopyable>(name, bpy::init<uint16_t>())
    .def(bpy::init<const kll_sketch<T>&>())
    .def("update", &kll_sketch<T>::update)
    .def("merge", &kll_sketch<T>::merge)
    .def("__str__", &dspy::KllSketch_toString<T>)
    .def("isEmpty", &kll_sketch<T>::is_empty)
    .def("getN", &kll_sketch<T>::get_n)
    .def("getNumRetained", &kll_sketch<T>::get_num_retained)
    .def("isEstimationMode", &kll_sketch<T>::is_estimation_mode)
    .def("getMinValue", &kll_sketch<T>::get_min_value)
    .def("getMaxValue", &kll_sketch<T>::get_max_value)
    .def("getQuantile", &kll_sketch<T>::get_quantile)
    .def("getQuantiles", &dspy::KllSketch_getQuantiles<T>)
    .def("getRank", &kll_sketch<T>::get_rank)
    .def("getPMF", &dspy::KllSketch_getPMF<T>)
    .def("getCDF", &dspy::KllSketch_getCDF<T>)
    .def("normalizedRankError", &dspy::KllSketch_sketchNormalizedRankError<T>)
    .def("getNormalizedRankError", &dspy::KllSketch_generalNormalizedRankError<T>)
    .staticmethod("getNormalizedRankError")
    .def("getSerializedSizeBytes", &dspy::KllSketch_getSerializedSizeBytes<T>)
    .def("getSizeofItem", &kll_sketch<T>::get_sizeof_item)
    .staticmethod("getSizeofItem")
    .def("getMaxSerializedSizeBytes", &kll_sketch<T>::get_max_serialized_size_bytes)
    .staticmethod("getMaxSerializedSizeBytes")
    .def("serialize", &dspy::KllSketch_serialize<T>)
    .def("deserialize", &dspy::KllSketch_deserialize<T>, bpy::return_value_policy<bpy::manage_new_object>())
    .staticmethod("deserialize")   
    ;
}

void export_kll()
{
  bind_kll_sketch<int>("KllIntSketch");
  bind_kll_sketch<float>("KllFloatSketch");
}