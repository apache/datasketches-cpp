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

#ifndef _VERSION_HPP_
#define _VERSION_HPP_

namespace datasketches {

// the configured options and settings for DataSketches
constexpr int VERSION_MAJOR {5};
constexpr int VERSION_MINOR {3};
constexpr int VERSION_PATCH {20250915};
constexpr int VERSION_TWEAK {2017};

constexpr auto VERSION_STR = "5.3.20250915.2017";
constexpr auto SOURCE_URL = "https://github.com/apache/datasketches-cpp";

}

#endif // _VERSION_HPP_
