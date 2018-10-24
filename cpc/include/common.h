/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

// author Kevin Lang, Oath Research

#ifndef GOT_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
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

/* some C syntax magic */
#define FATAL_ERROR(msg) \
  do { \
    fprintf (stderr, "Program failed because %s.\n", (msg)); \
    fflush (stderr); \
    exit(-1); \
  } while (0)


// enum animal {Horse, Pig, Cow};

#define GOT_COMMON_H
#endif


