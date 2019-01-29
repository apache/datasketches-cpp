/*
 * Copyright 2019, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "cpc_sketch.hpp"
#include "cpc_union.hpp"
#include "cpc_common.hpp"
#include <boost/python.hpp>

namespace bpy = boost::python;

namespace datasketches {
namespace python {

cpc_sketch* CpcSketch_deserialize(bpy::object obj) {
  PyObject* skBytes = obj.ptr();
  if (!PyBytes_Check(skBytes)) {
    PyErr_SetString(PyExc_TypeError, "Attmpted to deserialize non-bytes object");
    bpy::throw_error_already_set();
    return nullptr;
  }
  
  size_t len = PyBytes_GET_SIZE(skBytes);
  char* sketchImg = PyBytes_AS_STRING(skBytes);
  cpc_sketch_unique_ptr sk = cpc_sketch::deserialize(sketchImg, len);
  return sk.release();
}

bpy::object CpcSketch_serialize(const cpc_sketch& sk) {
  auto serResult = sk.serialize();
  PyObject* sketchBytes = PyBytes_FromStringAndSize((char*)serResult.first.get(), serResult.second);
  return bpy::object{bpy::handle<>(sketchBytes)};
}

std::string CpcSketch_toString(const cpc_sketch& sk) {
  std::ostringstream ss;
  ss << sk;
  return ss.str();
}

cpc_sketch* CpcUnion_getResult(const cpc_union& u) {
  auto sk = u.get_result();
  return sk.release();
}

}
}

namespace dspy = datasketches::python;

void export_cpc()
{
  using namespace datasketches;

  bpy::class_<cpc_sketch, boost::noncopyable>("CpcSketch", bpy::init<const cpc_sketch&>())
    .def(bpy::init<uint8_t>())
    .def(bpy::init<uint8_t, uint64_t>())
    .def("__str__", &dspy::CpcSketch_toString)
    .def("serialize", &dspy::CpcSketch_serialize)
    .def("deserialize", &dspy::CpcSketch_deserialize, bpy::return_value_policy<bpy::manage_new_object>())
    .staticmethod("deserialize")
    .def<void (cpc_sketch::*)(uint64_t)>("update", &cpc_sketch::update)
    .def<void (cpc_sketch::*)(int64_t)>("update", &cpc_sketch::update)
    .def<void (cpc_sketch::*)(double)>("update", &cpc_sketch::update)
    .def<void (cpc_sketch::*)(const std::string&)>("update", &cpc_sketch::update)
    .def("isEmpty", &cpc_sketch::is_empty)
    .def("getEstimate", &cpc_sketch::get_estimate)
    .def("getLowerBound", &cpc_sketch::get_lower_bound)
    .def("getUpperBound", &cpc_sketch::get_upper_bound)
    .def("getEstimate", &cpc_sketch::get_estimate)
    ;

  bpy::class_<cpc_union, boost::noncopyable>("CpcUnion", bpy::init<const cpc_union&>())
    .def(bpy::init<uint8_t>())
    .def(bpy::init<uint8_t, uint64_t>())
    .def("update", &cpc_union::update)
    .def("getResult", &dspy::CpcUnion_getResult, bpy::return_value_policy<bpy::manage_new_object>())
    ;
}