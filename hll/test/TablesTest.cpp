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

#include "CubicInterpolation.hpp"

#include <cppunit/TestFixture.h>
#include <cppunit/extensions/HelperMacros.h>

namespace datasketches {

class TablesTest : public CppUnit::TestFixture {

  CPPUNIT_TEST_SUITE(TablesTest);
  CPPUNIT_TEST(interpolationException);
  CPPUNIT_TEST(checkCornerCase);
  CPPUNIT_TEST_SUITE_END();

  void interpolationException() {
    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to throw with x = -1.0",
                                 CubicInterpolation<>::usingXAndYTables(-1.0),
                                 std::invalid_argument);

    CPPUNIT_ASSERT_THROW_MESSAGE("Failed to throw with x = 1e12",
                                 CubicInterpolation<>::usingXAndYTables(1e12),
                                 std::invalid_argument);
  }

  void checkCornerCase() {
    int len = 10;
    double xArr[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
    double yArr[] = {2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0};
    double x = xArr[len - 1];
    double y = CubicInterpolation<>::usingXAndYTables(xArr, yArr, len, x);
    double yExp = yArr[len - 1];
    CPPUNIT_ASSERT_DOUBLES_EQUAL(yExp, y, 0.0);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(TablesTest);

} /* namespace datasketches */

