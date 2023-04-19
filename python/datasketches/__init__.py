# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""The Apache DataSketches Library for Python

Provided under the Apache License, Verison 2.0
<http://www.apache.org/licenses/LICENSE-2.0>
"""

name = 'datasketches'

from _datasketches import *

from .PySerDe import *
from .TuplePolicy import *
from .KernelFunction import *

# Wrappers around the pybind11 classes for cases where we
# need to define a python object that is persisted within
# the C++ object. Currently, the native python portion of
# a class derived from a C++ class may be garbage collected
# even though a pointer to the C++ portion remains valid.
from .TupleWrapper import *
from .DensityWrapper import *