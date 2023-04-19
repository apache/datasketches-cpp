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

#ifndef _PY_OBJECT_OSTREAM_HPP_
#define _PY_OBJECT_OSTREAM_HPP_

#include <pybind11/pybind11.h>

#include <string>
#include <ostream>

/*
  This header defines an ostream output operator on a generic python
  object. The implementation calls the object's built-in __str__()
  method. If that method is not defined, the call may fail.

  NOTE: This header must be included before the inclusion of
        any sketch classes.
*/

namespace py = pybind11;

namespace datasketches {

static std::ostream& operator<<(std::ostream& os, const py::object& obj) {
  os << std::string(pybind11::str(obj));
  return os;
}

}

#endif // _PY_OBJECT_OSTREAM_HPP_
