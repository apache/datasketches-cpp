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

// author Kevin Lang, Oath Research

#ifndef GOT_FM85_CONFIDENCE_H

#include "common.h"
#include "fm85.h"

double getIconConfidenceLB (FM85 * sketch, int kappa);
double getIconConfidenceUB (FM85 * sketch, int kappa);
double getHIPConfidenceLB  (FM85 * sketch, int kappa);
double getHIPConfidenceUB  (FM85 * sketch, int kappa);

#define GOT_FM85_CONFIDENCE_H
#endif
