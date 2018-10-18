/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

// author Kevin Lang, Oath Research

#ifndef GOT_FM85_UTIL_H
#include "common.h"

void * shallowCopy (void * oldObject, size_t numBytes);

extern double invPow2Tab[];

void fillInvPow2Tab (void);

extern double kxpByteLookup[];

void fillKxpByteLookup(void);

extern U8 byteTrailingZerosTable[];

void fillByteLeadingZerosTable(void);
void fillByteTrailingZerosTable(void);

Short countLeadingZerosInUnsignedLong  (U64 theInput);
Short countTrailingZerosInUnsignedLong (U64 theInput);

Long divideLongsRoundingUp (Long x, Long y);

// for delta-encoding an instance of (n choose m)
Long golombChooseNumberOfBaseBits (Long n, Long m);

Long countBitsSetInMatrix (U64 * array, Long length);

/******************************************/

#define GOT_FM85_UTIL_H
#endif


