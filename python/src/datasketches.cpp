/*
 * Copyright 2019, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

#include <boost/python.hpp>

void export_hll();
void export_kll();
void export_cpc();
void export_fi();

BOOST_PYTHON_MODULE(datasketches) {
  export_hll();
  export_kll();
  export_cpc();
  export_fi();
}
