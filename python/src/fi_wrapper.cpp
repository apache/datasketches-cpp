/*
 * Copyright 2019, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include "frequent_items_sketch.hpp"
#include <boost/python.hpp>

namespace bpy = boost::python;

namespace datasketches {
namespace python {

template<typename T>
frequent_items_sketch<T>* FISketch_deserialize(bpy::object obj) {
  PyObject* skBytes = obj.ptr();
  if (!PyBytes_Check(skBytes)) {
    PyErr_SetString(PyExc_TypeError, "Attmpted to deserialize non-bytes object");
    bpy::throw_error_already_set();
    return nullptr;
  }
  
  size_t len = PyBytes_GET_SIZE(skBytes);
  char* sketchImg = PyBytes_AS_STRING(skBytes);
  auto sk = frequent_items_sketch<T>::deserialize(sketchImg, len);
  return std::move(&sk);
}

template<typename T>
bpy::object FISketch_serialize(const frequent_items_sketch<T>& sk) {
  auto serResult = sk.serialize();
  PyObject* sketchBytes = PyBytes_FromStringAndSize((char*)serResult.first.get(), serResult.second);
  return bpy::object{bpy::handle<>(sketchBytes)};
}

template<typename T>
double FISketch_getSketchEpsilon(const frequent_items_sketch<T>& sk) {
  return sk.get_epsilon();
}

template<typename T>
double FISketch_getGenericEpsilon(uint8_t lg_max_map_size) {
  return frequent_items_sketch<T>::get_epsilon(lg_max_map_size);
}

template<typename T>
void FISketch_update(frequent_items_sketch<T>& sk,
                     const T& item,
                     uint64_t weight = 1) {
  sk.update(item, weight);
}

template<typename T>
bpy::list FISketch_getFrequentItems(const frequent_items_sketch<T>& sk,
                                    frequent_items_error_type err_type,
                                    uint64_t threshold = 0) {
  if (threshold == 0) { threshold = sk.get_maximum_error(); }

  bpy::list list;
  auto items = sk.get_frequent_items(err_type, threshold);
  for (auto iter = items.begin(); iter != items.end(); ++iter) {
    bpy::tuple t = bpy::make_tuple(iter->get_item(),
                                   iter->get_estimate(),
                                   iter->get_lower_bound(),
                                   iter->get_upper_bound());
    list.append(t);
  }
  return list;
}

template<typename T>
std::string FISketch_toString(const frequent_items_sketch<T>& sk,
                              bool print_items = false) {
  std::ostringstream ss;
  sk.to_stream(ss, print_items);
  return ss.str();
}

}

}

namespace dspy = datasketches::python;

BOOST_PYTHON_FUNCTION_OVERLOADS(FISketchUpdateOverloads, dspy::FISketch_update, 2, 3)
BOOST_PYTHON_FUNCTION_OVERLOADS(FISketchGetFrequentItemsOverloads, dspy::FISketch_getFrequentItems, 2, 3)
BOOST_PYTHON_FUNCTION_OVERLOADS(FISketchToStringOverloads, dspy::FISketch_toString, 1, 2)

template<typename T>
void bind_fi_sketch(const char* name)
{
  using namespace datasketches;

  bpy::class_<frequent_items_sketch<T>, boost::noncopyable>(name, bpy::init<uint8_t>())
    .def("__str__", &dspy::FISketch_toString<T>, FISketchToStringOverloads())
    .def("to_string", &dspy::FISketch_toString<T>, FISketchToStringOverloads())
    .def("update", &dspy::FISketch_update<T>, FISketchUpdateOverloads())
    .def("get_frequent_items", &dspy::FISketch_getFrequentItems<T>, FISketchGetFrequentItemsOverloads())
    .def("merge", &frequent_items_sketch<T>::merge)
    .def("is_empty", &frequent_items_sketch<T>::is_empty)
    .def("get_num_active_items", &frequent_items_sketch<T>::get_num_active_items)
    .def("get_total_weight", &frequent_items_sketch<T>::get_total_weight)
    .def("get_estimate", &frequent_items_sketch<T>::get_estimate)
    .def("get_lower_bound", &frequent_items_sketch<T>::get_lower_bound)
    .def("get_upper_bound", &frequent_items_sketch<T>::get_upper_bound)
    .def("get_sketch_epsilon", &dspy::FISketch_getSketchEpsilon<T>)
    .def("get_epsilon_for_lg_size", &dspy::FISketch_getGenericEpsilon<T>)
    .staticmethod("get_epsilon_for_lg_size")
    .def("get_apriori_error", &frequent_items_sketch<T>::get_apriori_error)
    .staticmethod("get_apriori_error")
    .def("get_serialized_size_bytes", &frequent_items_sketch<T>::get_serialized_size_bytes)
    .def("serialize", &dspy::FISketch_serialize<T>)
    .def("deserialize", &dspy::FISketch_deserialize<T>, bpy::return_value_policy<bpy::manage_new_object>())
    .staticmethod("deserialize")
    ;
}

void export_fi()
{
  using namespace datasketches;

  bpy::enum_<frequent_items_error_type>("frequent_items_error_type")
    .value("NO_FALSE_POSITIVES", NO_FALSE_POSITIVES)
    .value("NO_FALSE_NEGATIVES", NO_FALSE_NEGATIVES)
    ;

  bind_fi_sketch<std::string>("FrequentStringsSketch");
}