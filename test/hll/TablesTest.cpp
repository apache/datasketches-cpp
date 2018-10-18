/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
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
    try {
      CubicInterpolation::usingXAndYTables(-1.0);
      CPPUNIT_FAIL("usingXAndYTables must throw with x = -1.0");
    } catch (std::exception& e) {
      // expected
    }

    try {
      CubicInterpolation::usingXAndYTables(1e12);
      CPPUNIT_FAIL("usingXAndYTables must throw with x = 1e12");
    } catch (std::exception& e) {
      // expected
    }
  }

  void checkCornerCase() {
    int len = 10;
    double xArr[] = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};
    double yArr[] = {2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0};
    double x = xArr[len - 1];
    double y = CubicInterpolation::usingXAndYTables(xArr, yArr, len, x);
    double yExp = yArr[len - 1];
    CPPUNIT_ASSERT_DOUBLES_EQUAL(yExp, y, 0.0);
  }

};

CPPUNIT_TEST_SUITE_REGISTRATION(TablesTest);

} /* namespace datasketches */

