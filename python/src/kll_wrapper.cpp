/*
 * Copyright 2019, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "kll/include/kll_sketch.hpp"
#include <boost/python.hpp>

namespace bpy = boost::python;

typedef datasketches::kll_sketch<int> KllIntSketch;

namespace datasketches {
namespace python {

KllIntSketch* KllIntSketch_deserialize(bpy::object obj) {
  PyObject* skBytes = obj.ptr();
  if (!PyBytes_Check(skBytes)) {
    // TODO: return error?
    return nullptr;
  }
  
  size_t len = PyBytes_GET_SIZE(skBytes);
  char* sketchImg = PyBytes_AS_STRING(skBytes);
  auto sk = KllIntSketch::deserialize(sketchImg, len);
  return sk.release();
}

bpy::object KllIntSketch_serialize(const KllIntSketch& sk) {
  auto serResult = sk.serialize();
  PyObject* sketchBytes = PyBytes_FromStringAndSize((char*)serResult.first.get(), serResult.second);
  return bpy::object{bpy::handle<>(sketchBytes)};
}

double KllIntSketch_sketchNormalizedRankError(const KllIntSketch& sk,
                                              bool pmf) {
  return sk.get_normalized_rank_error(pmf);
}

double KllIntSketch_generalNormalizedRankError(uint16_t k, bool pmf) {
  return KllIntSketch::get_normalized_rank_error(k, pmf);
}

bpy::list KllIntSketch_getQuantiles(const KllIntSketch& sk,
                                    bpy::list& fractions) {
  size_t nQuantiles = len(fractions);
  double* frac = new double[nQuantiles];
  for (int i = 0; i < nQuantiles; ++i) {
    frac[i] = bpy::extract<double>(fractions[i]);
  }
  std::unique_ptr<int[]> result = sk.get_quantiles(frac, nQuantiles);

  PyObject* list = PyList_New(nQuantiles);
  for (int i = 0; i < nQuantiles; ++i) {
    PyList_SET_ITEM(list, i, PyLong_FromLong(result[i]));
  }

  delete [] frac;
  return bpy::list{bpy::handle<>(list)};
}

bpy::list KllIntSketch_getPMF(const KllIntSketch& sk,
                              bpy::list& split_points) {
  size_t nPoints = len(split_points);
  int* splitPoints = new int[nPoints];
  for (int i = 0; i < nPoints; ++i) {
    splitPoints[i] = bpy::extract<int>(split_points[i]);
  }
  std::unique_ptr<double[]> result = sk.get_PMF(splitPoints, nPoints);

  PyObject* pmf = PyList_New(nPoints);
  for (int i = 0; i < nPoints; ++i) {
    PyList_SET_ITEM(pmf, i, PyFloat_FromDouble(result[i]));
  }

  delete [] splitPoints;
  return bpy::list{bpy::handle<>(pmf)};
}

bpy::list KllIntSketch_getCDF(const KllIntSketch& sk,
                              bpy::list& split_points) {
  size_t nPoints = len(split_points);
  int* splitPoints = new int[nPoints];
  for (int i = 0; i < nPoints; ++i) {
    splitPoints[i] = bpy::extract<int>(split_points[i]);
  }
  std::unique_ptr<double[]> result = sk.get_CDF(splitPoints, nPoints);

  PyObject* cdf = PyList_New(nPoints);
  for (int i = 0; i < nPoints; ++i) {
    PyList_SET_ITEM(cdf, i, PyFloat_FromDouble(result[i]));
  }

  delete [] splitPoints;
  return bpy::list{bpy::handle<>(cdf)};
}

}
}

namespace dspy = datasketches::python;

/*
template<typename T>
void export_generic_kll()
{
  bpy::class_<kll_sketch<T>>("KllSketch", bpy::init<uint16_t>()),
    .def(bpy::init<const kll_sketch&>()))
    ;
}
*/

void export_kll()
{
  using namespace datasketches;

  bpy::class_<kll_sketch<int>>("KllIntSketch", bpy::init<uint16_t>())
    .def(bpy::init<const KllIntSketch&>())
    .def("update", &KllIntSketch::update)
    .def("merge", &KllIntSketch::merge)
    .def("isEmpty", &KllIntSketch::is_empty)
    .def("getN", &KllIntSketch::get_n)
    .def("getNumRetained", &KllIntSketch::get_num_retained)
    .def("isEstimationMode", &KllIntSketch::is_estimation_mode)
    .def("getMinValue", &KllIntSketch::get_min_value)
    .def("getMaxValue", &KllIntSketch::get_max_value)
    .def("getQuantile", &KllIntSketch::get_quantile)
    .def("getQuantiles", &dspy::KllIntSketch_getQuantiles)
    .def("getRank", &KllIntSketch::get_rank)
    .def("getPMF", &dspy::KllIntSketch_getPMF)
    .def("getCDF", &dspy::KllIntSketch_getCDF)
    .def("normalizedRankError", &dspy::KllIntSketch_sketchNormalizedRankError)
    .def("getNormalizedRankError", &dspy::KllIntSketch_generalNormalizedRankError)
    .staticmethod("getNormalizedRankError")
    //.def("getSerializedSizeBytes", &KllIntSketch::get_serialized_size_bytes)
    .def("getSizeofItem", &KllIntSketch::get_sizeof_item)
    .staticmethod("getSizeofItem")
    .def("getMaxSerializedSizeBytes", &KllIntSketch::get_max_serialized_size_bytes)
    .staticmethod("getMaxSerializedSizeBytes")
    .def("serialize", &dspy::KllIntSketch_serialize)
    .def("deserialize", &dspy::KllIntSketch_deserialize, bpy::return_value_policy<bpy::manage_new_object>())
    .staticmethod("deserialize")
    ;

}