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

#ifndef GOT_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>

typedef int Boolean;

typedef u_int8_t  U8;
typedef u_int16_t U16;
typedef u_int32_t U32;
typedef u_int64_t U64;

typedef int16_t Short; // signed
typedef int64_t Long;  // signed

#define ALL64BITS 0xffffffffffffffffULL
#define ALL32BITS 0xffffffffULL

// Do not use either of these with a shift of 0 or 64.
#define ROTATE_RIGHT_MACRO(val,shift) (((val) >> (shift)) | ((val) << (64 - (shift))))
#define ROTATE_LEFT_MACRO(val,shift)  (((val) << (shift)) | ((val) >> (64 - (shift))))

#define GOT_COMMON_H
#endif


