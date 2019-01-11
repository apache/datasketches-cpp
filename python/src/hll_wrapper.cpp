#include "hll/include/hll.hpp"
#include <boost/python.hpp>

namespace bpy = boost::python;
namespace ds = datasketches;

// wrappers for methods returning std::unique_ptr
ds::HllSketch* HllSketch_newInstance(int lgConfigK,
                                       ds::TgtHllType tgtHllType = ds::HLL_4) {
  ds::hll_sketch sk = ds::HllSketch::newInstance(lgConfigK, tgtHllType);
  return sk.release();
}

ds::HllSketch* HllSketch_deserialize(bpy::object obj) {
  PyObject* mv = obj.ptr();
  if (!PyMemoryView_Check(mv)) {
    // TODO: return error
  }
  
  Py_buffer* buffer = PyMemoryView_GET_BUFFER(mv);
  ds::hll_sketch sk = ds::HllSketch::deserialize(buffer->buf, buffer->len);
  PyBuffer_Release(buffer);
  return sk.release();
}

PyObject* HllSketch_serializeCompact(const ds::HllSketch& sk) {
  std::pair<std::unique_ptr<uint8_t>, const size_t> bytes = sk.serializeCompact();
  char* ptr = (char*)(bytes.first.release());
  PyObject* mv = PyMemoryView_FromMemory(ptr, bytes.second, PyBUF_READ);
  return mv;
}

PyObject* HllSketch_serializeUpdatable(const ds::HllSketch& sk) {
  std::pair<std::unique_ptr<uint8_t>, const size_t> bytes = sk.serializeUpdatable();
  char* ptr = (char*)(bytes.first.release());
  PyObject* mv = PyMemoryView_FromMemory(ptr, bytes.second, PyBUF_READ);
  return mv;
}

std::string HllSketch_toString(const ds::HllSketch& sk,
                               bool summary = true,
                               bool detail = false,
                               bool auxDetail = false,
                               bool all = false) {
  return sk.to_string(summary, detail, auxDetail, all);
}

std::string HllSketch_toStringDefault(const ds::HllSketch& sk) {
  return HllSketch_toString(sk);
}

BOOST_PYTHON_FUNCTION_OVERLOADS(HllSketchNewInstanceOverloads, HllSketch_newInstance, 1, 2);
BOOST_PYTHON_FUNCTION_OVERLOADS(HllSketchToStringOverloads, HllSketch_toString, 1, 5);

BOOST_PYTHON_MODULE(pyhll)
{
  using namespace datasketches;

  bpy::enum_<ds::TgtHllType>("TgtHllType")
    .value("HLL_4", HLL_4)
    .value("HLL_6", HLL_6)
    .value("HLL_8", HLL_8)
    ;

  bpy::class_<HllSketch, boost::noncopyable>("HllSketch", bpy::no_init)
    .def("newInstance", &HllSketch_newInstance, HllSketchNewInstanceOverloads()[bpy::return_value_policy<bpy::manage_new_object>()])
    .staticmethod("newInstance")
    .def("deserialize", &HllSketch_deserialize, bpy::return_value_policy<bpy::manage_new_object>())
    .staticmethod("deserialize")
    .def("serializeCompact", &HllSketch_serializeCompact)
    .def("serializeUpdatable", &HllSketch_serializeUpdatable)
    .def("__str__", &HllSketch_toStringDefault)
    .add_property("lgConfigK", &HllSketch::getLgConfigK)
    .add_property("tgtHllType", &HllSketch::getTgtHllType)
    .def("to_string", &HllSketch_toString, HllSketchToStringOverloads())
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
    ;
}