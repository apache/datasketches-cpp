/*
 * Copyright 2019, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "hll/include/hll.hpp"
#include <boost/python.hpp>

namespace bpy = boost::python;

namespace datasketches {
namespace python {

HllSketch* HllSketch_newInstance(int lgConfigK,
                                 TgtHllType tgtHllType = HLL_4) {
  hll_sketch sk = HllSketch::newInstance(lgConfigK, tgtHllType);
  return sk.release();
}

HllSketch* HllSketch_deserialize(bpy::object obj) {
  PyObject* mv = obj.ptr();
  if (!PyMemoryView_Check(mv)) {
    // TODO: return error?
    return nullptr;
  }
  
  Py_buffer* buffer = PyMemoryView_GET_BUFFER(mv);
  hll_sketch sk = HllSketch::deserialize(buffer->buf, buffer->len);
  PyBuffer_Release(buffer);
  return sk.release();
}

PyObject* HllSketch_serializeCompact(const HllSketch& sk) {
  std::pair<std::unique_ptr<uint8_t>, const size_t> bytes = sk.serializeCompact();
  char* ptr = (char*)(bytes.first.release());
  PyObject* mv = PyMemoryView_FromMemory(ptr, bytes.second, PyBUF_READ);
  return mv;
}

PyObject* HllSketch_serializeUpdatable(const HllSketch& sk) {
  std::pair<std::unique_ptr<uint8_t>, const size_t> bytes = sk.serializeUpdatable();
  char* ptr = (char*)(bytes.first.release());
  PyObject* mv = PyMemoryView_FromMemory(ptr, bytes.second, PyBUF_READ);
  return mv;
}

std::string HllSketch_toString(const HllSketch& sk,
                               bool summary = true,
                               bool detail = false,
                               bool auxDetail = false,
                               bool all = false) {
  return sk.to_string(summary, detail, auxDetail, all);
}

std::string HllSketch_toStringDefault(const HllSketch& sk) {
  return HllSketch_toString(sk);
}

HllUnion* HllUnion_newInstance(int lgMaxK) {
  hll_union u = HllUnion::newInstance(lgMaxK);
  return u.release();
}

HllUnion* HllUnion_deserialize(bpy::object obj) {
  PyObject* mv = obj.ptr();
    if (!PyMemoryView_Check(mv)) {
    // TODO: return error?
    return nullptr;
  }
  
  Py_buffer* buffer = PyMemoryView_GET_BUFFER(mv);
  hll_union u = HllUnion::deserialize(buffer->buf, buffer->len);
  PyBuffer_Release(buffer);
  return u.release();
}

PyObject* HllUnion_serializeCompact(const HllUnion& u) {
  std::pair<std::unique_ptr<uint8_t>, const size_t> bytes = u.serializeCompact();
  char* ptr = (char*)(bytes.first.release());
  PyObject* mv = PyMemoryView_FromMemory(ptr, bytes.second, PyBUF_READ);
  return mv;
}

PyObject* HllUnion_serializeUpdatable(const HllUnion& u) {
  std::pair<std::unique_ptr<uint8_t>, const size_t> bytes = u.serializeUpdatable();
  char* ptr = (char*)(bytes.first.release());
  PyObject* mv = PyMemoryView_FromMemory(ptr, bytes.second, PyBUF_READ);
  return mv;
}

std::string HllUnion_toString(const HllUnion& u,
                              bool summary = true,
                              bool detail = false,
                              bool auxDetail = false,
                              bool all = false) {
  return u.to_string(summary, detail, auxDetail, all);
}

std::string HllUnion_toStringDefault(const HllUnion& u) {
  return HllUnion_toString(u);
}

HllSketch* HllUnion_getResult(const HllUnion& u,
                                    TgtHllType tgtHllType = HLL_4) {
  return u.getResult(tgtHllType).release();
}

}
}

namespace dspy = datasketches::python;

BOOST_PYTHON_FUNCTION_OVERLOADS(HllSketchNewInstanceOverloads, dspy::HllSketch_newInstance, 1, 2);
BOOST_PYTHON_FUNCTION_OVERLOADS(HllSketchToStringOverloads, dspy::HllSketch_toString, 1, 5);

BOOST_PYTHON_FUNCTION_OVERLOADS(HllUnionToStringOverloads, dspy::HllUnion_toString, 1, 5);
BOOST_PYTHON_FUNCTION_OVERLOADS(HllUnionGetResultOverloads, dspy::HllUnion_getResult, 1, 2);

void export_hll()
{
  using namespace datasketches;

  bpy::enum_<TgtHllType>("TgtHllType")
    .value("HLL_4", HLL_4)
    .value("HLL_6", HLL_6)
    .value("HLL_8", HLL_8)
    ;

  bpy::class_<HllSketch, boost::noncopyable>("HllSketch", bpy::no_init)
    .def("newInstance", &dspy::HllSketch_newInstance, HllSketchNewInstanceOverloads()[bpy::return_value_policy<bpy::manage_new_object>()])
    .staticmethod("newInstance")
    .def("deserialize", &dspy::HllSketch_deserialize, bpy::return_value_policy<bpy::manage_new_object>())
    .staticmethod("deserialize")
    .def("serializeCompact", &dspy::HllSketch_serializeCompact)
    .def("serializeUpdatable", &dspy::HllSketch_serializeUpdatable)
    .def("__str__", &dspy::HllSketch_toStringDefault)
    .add_property("lgConfigK", &HllSketch::getLgConfigK)
    .add_property("tgtHllType", &HllSketch::getTgtHllType)
    .def("to_string", &dspy::HllSketch_toString, HllSketchToStringOverloads())
    .def("getEstimate", &HllSketch::getEstimate)
    .def("getCompositeEstimate", &HllSketch::getCompositeEstimate)
    .def("getLowerBound", &HllSketch::getLowerBound)
    .def("getUpperBound", &HllSketch::getUpperBound)
    .def("isCompact", &HllSketch::isCompact)
    .def("isEmpty", &HllSketch::isEmpty)
    .def("getUpdtableSerialiationBytes", &HllSketch::getUpdatableSerializationBytes)
    .def("getCompactSerialiationBytes", &HllSketch::getCompactSerializationBytes)
    .def("reset", &HllSketch::reset)
    .def<void (HllSketch::*)(uint64_t)>("update", &HllSketch::update)
    .def<void (HllSketch::*)(int64_t)>("update", &HllSketch::update)
    .def<void (HllSketch::*)(double)>("update", &HllSketch::update)
    .def<void (HllSketch::*)(const std::string&)>("update", &HllSketch::update)
    .def<void (HllSketch::*)(const void*, size_t)>("update", &HllSketch::update)
    .def("getMaxUpdatableSerializationBytes", &HllSketch::getMaxUpdatableSerializationBytes)
    .staticmethod("getMaxUpdatableSerializationBytes")
    .def("getRelErr", &HllSketch::getRelErr)
    .staticmethod("getRelErr")
    ;

  bpy::class_<HllUnion, boost::noncopyable>("HllUnion", bpy::no_init)
    .def("newInstance", &dspy::HllUnion_newInstance, bpy::return_value_policy<bpy::manage_new_object>())
    .staticmethod("newInstance")
    .def("deserialize", &dspy::HllUnion_deserialize, bpy::return_value_policy<bpy::manage_new_object>())
    .staticmethod("deserialize")
    .def("serializeCompact", &dspy::HllUnion_serializeCompact)
    .def("serializeUpdatable", &dspy::HllUnion_serializeUpdatable)
    .def("__str__", &dspy::HllUnion_toStringDefault)
    .add_property("lgConfigK", &HllUnion::getLgConfigK)
    .add_property("tgtHllType", &HllUnion::getTgtHllType)
    .def("to_string", &dspy::HllUnion_toString, HllUnionToStringOverloads())
    .def("getEstimate", &HllUnion::getEstimate)
    .def("getCompositeEstimate", &HllUnion::getCompositeEstimate)
    .def("getLowerBound", &HllUnion::getLowerBound)
    .def("getUpperBound", &HllUnion::getUpperBound)
    .def("isCompact", &HllUnion::isCompact)
    .def("isEmpty", &HllUnion::isEmpty)
    .def("getUpdtableSerialiationBytes", &HllUnion::getUpdatableSerializationBytes)
    .def("getCompactSerialiationBytes", &HllUnion::getCompactSerializationBytes)
    .def("reset", &HllUnion::reset)
    .def("getResult", &dspy::HllUnion_getResult, HllUnionGetResultOverloads()[bpy::return_value_policy<bpy::manage_new_object>()])
    .def<void (HllUnion::*)(const HllSketch&)>("update", &HllUnion::update)
    .def<void (HllUnion::*)(uint64_t)>("update", &HllUnion::update)
    .def<void (HllUnion::*)(int64_t)>("update", &HllUnion::update)
    .def<void (HllUnion::*)(double)>("update", &HllUnion::update)
    .def<void (HllUnion::*)(const std::string&)>("update", &HllUnion::update)
    .def<void (HllUnion::*)(const void*, size_t)>("update", &HllUnion::update)
    .def("getMaxSerializationBytes", &HllUnion::getMaxSerializationBytes)
    .staticmethod("getMaxSerializationBytes")
    .def("getRelErr", &HllUnion::getRelErr)
    .staticmethod("getRelErr")

    ;
}