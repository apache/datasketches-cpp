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

#include "fm85Util.h"

#include <stdexcept>
#include <new>

extern void* (*fm85alloc)(size_t);

/******************************************/

void * shallowCopy (void * oldObject, size_t numBytes) {
  if (oldObject == NULL || numBytes == 0) throw std::invalid_argument("shallowCopyObject: bad argument");
  void * newObject = fm85alloc (numBytes);
  if (newObject == NULL) throw std::bad_alloc();
  memcpy (newObject, oldObject, numBytes);
  return (newObject);
}

/******************************************/

U8 byteLeadingZerosTable[256];

U8 slow_count_leading_zeros_in_byte (U8 the_byte)
{
  int shift;
  int count = 0;
  for (shift = 7; shift >= 0; shift--) {
    int is_zero = !((the_byte >> shift) & 1);
    if (is_zero)
      count++;
    else
      break;
  }
  return count;
}

void fillByteLeadingZerosTable(void)
{
  int j;
  for (j = 0; j < 256; j++)
    byteLeadingZerosTable[j] = slow_count_leading_zeros_in_byte ((U8) j);
}

/******************************************/

#define FCLZ_MASK_56 ((U64) 0x00ffffffffffffff)
#define FCLZ_MASK_48 ((U64) 0x0000ffffffffffff)
#define FCLZ_MASK_40 ((U64) 0x000000ffffffffff)
#define FCLZ_MASK_32 ((U64) 0x00000000ffffffff)
#define FCLZ_MASK_24 ((U64) 0x0000000000ffffff)
#define FCLZ_MASK_16 ((U64) 0x000000000000ffff)
#define FCLZ_MASK_08 ((U64) 0x00000000000000ff)

Short countLeadingZerosInUnsignedLong (U64 theInput) {
  if (theInput > FCLZ_MASK_56)
    return ((Short) ( 0 + byteLeadingZerosTable[(theInput >> 56) & FCLZ_MASK_08]));
  if (theInput > FCLZ_MASK_48)
    return ((Short) ( 8 + byteLeadingZerosTable[(theInput >> 48) & FCLZ_MASK_08]));
  if (theInput > FCLZ_MASK_40)
    return ((Short) (16 + byteLeadingZerosTable[(theInput >> 40) & FCLZ_MASK_08]));
  if (theInput > FCLZ_MASK_32)
    return ((Short) (24 + byteLeadingZerosTable[(theInput >> 32) & FCLZ_MASK_08]));
  if (theInput > FCLZ_MASK_24)
    return ((Short) (32 + byteLeadingZerosTable[(theInput >> 24) & FCLZ_MASK_08]));
  if (theInput > FCLZ_MASK_16)
    return ((Short) (40 + byteLeadingZerosTable[(theInput >> 16) & FCLZ_MASK_08]));
  if (theInput > FCLZ_MASK_08)
    return ((Short) (48 + byteLeadingZerosTable[(theInput >>  8) & FCLZ_MASK_08]));
  if (1)
    return ((Short) (56 + byteLeadingZerosTable[(theInput >>  0) & FCLZ_MASK_08]));
}

/******************************************/

U8 byteTrailingZerosTable[256];

// U8 lookupByteTrailingZeros (int x) { return (byteTrailingZerosTable[x]); }

U8 slow_count_trailing_zeros_in_byte (U8 the_byte)
{
  int shift;
  int count = 0;
  for (shift = 0; shift <= 7; shift++) {
    int is_zero = !((the_byte >> shift) & 1);
    if (is_zero)
      count++;
    else
      break;
  }
  return count;
}

void fillByteTrailingZerosTable(void)
{
  int j;
  for (j = 0; j < 256; j++)
    byteTrailingZerosTable[j] = slow_count_trailing_zeros_in_byte ((U8) j);    
}

Short countTrailingZerosInUnsignedLong (U64 theInput) {
  U64 tmp = theInput;
  int byte;
  int j = 0;
  for (j = 0; j < 8; j++) {
    byte = (tmp & 0xffULL);
    if (byte != 0) return ((Short) ((j << 3) + byteTrailingZerosTable[byte]));
    tmp >>= 8;
  }
  return ((Short) (64));
}

/******************************************/

double invPow2Tab[256];

void fillInvPow2Tab (void)
{
  int j;
  for (j = 0; j < 256; j++) {
    invPow2Tab[j] = pow (2.0, (-1.0 * ((double) j)));
  }
}

/******************************************/

double kxpByteLookup[256];

void fillKxpByteLookup (void) // must call fillInvPow2Tab() first
{
  int byte, col;
  for (byte = 0; byte < 256; byte++) {
    double sum = 0.0;
    for (col = 0; col < 8; col++) {
      int bit = (byte >> col) & 1;
      if (bit == 0) { // note the inverted logic
	sum += invPow2Tab[col+1]; // note the "+1"
      }
    }      
    kxpByteLookup[byte] = sum;
  }
}


/******************************************/

Long divideLongsRoundingUp (Long x, Long y) {
  if (x < 0 || y <= 0) throw std::invalid_argument("divideLongsRoundingUp: bad argument");
  Long quotient = x / y;
  if (quotient * y == x) return (quotient);
  else return (quotient + 1);
}

Long longFloorLog2OfLong (Long x) {
  if (x < 1L) throw std::invalid_argument("longFloorLog2OfLong: bad argument");
  Long p = 0;
  Long y = 1;
 log2Loop:
  if (y == x) return (p);
  if (y  > x) return (p-1);
  p  += 1;
  y <<= 1;
  goto log2Loop;
}

/***********************************/

// returns an integer that is between 
// zero and ceiling(log_2(k))-1, inclusive

Long golombChooseNumberOfBaseBits (Long k, Long count) {
  if (k < 1L) throw std::invalid_argument("golombChooseNumberOfBaseBits: k < 1");
  if (count < 1L) throw std::invalid_argument("golombChooseNumberOfBaseBits: count < 1");
  Long quotient = (k - count) / count; // integer division
  if (quotient == 0) return (0);
  else return (longFloorLog2OfLong(quotient));
}


/*******************************************************/
// This place-holder code was inadequate because it caused
// the cost of the post-merge getResult() operation to be O(C)
// instead of O(K). It did have the advantage of being
// very simple and trustworthy during initial testing.

Long wegnerCountBitsSetInMatrix (U64 * array, Long length) {
  Long i = 0;
  U64 pattern = 0;
  Long count = 0;
  //  clock_t t0, t1;
  //  t0 = clock();
// Wegner's Bit-Counting Algorithm, CACM 3 (1960), p. 322.
  for (i = 0; i < length; i++) {
    pattern = array[i];
    while (pattern != 0) { 
      pattern &= (pattern - 1); 
      count++;
    }
  }
  //  t1 = clock();
  //  printf ("\n(Wegner CountBitsTime %.1f)\n", ((double) (t1 - t0)) / 1000.0);
  //  fflush (stdout);

  return count;
}

/*******************************************************/
// Note: this is an adaptation of the Java code that Lee sent me,
// which is apparently a variation of Figure 5-2 in "Hacker's Delight"
// by Henry S. Warren.

static inline Long warrenBitCount(U64 i) {
  i = i - ((i >> 1) & 0x5555555555555555ULL);
  i = (i & 0x3333333333333333ULL) + ((i >> 2) & 0x3333333333333333ULL);
  i = (i + (i >> 4)) & 0x0f0f0f0f0f0f0f0fULL;
  i = i + (i >> 8);
  i = i + (i >> 16);
  i = i + (i >> 32);
  return (Long)i & 0x7f;
}

Long warrenCountBitsSetInMatrix (U64 * array, Long length) {
  Long i = 0;
  Long count = 0;
  for (i = 0; i < length; i++) {
    count += warrenBitCount(array[i]);
  }
  return count;
}

/*******************************************************/
// This code is Figure 5-9 in "Hacker's Delight" by Henry S. Warren.

#define CSA(h,l,a,b,c) {U64 u = a^b; U64 v = c; h = (a&b) | (u&v); l = u^v;}

Long countBitsSetInMatrix (U64 * A, Long length) {
  if ((length & 0x7) != 0) throw std::invalid_argument("the length of the array must be a multiple of 8");
  Long tot, i;
  U64 ones, twos, twosA, twosB, fours, foursA, foursB, eights;
  tot = 0;
  fours = twos = ones = 0;

  for (i = 0; i <= length - 8; i = i + 8) {
    CSA(twosA, ones, ones, A[i+0], A[i+1]);
    CSA(twosB, ones, ones, A[i+2], A[i+3]);
    CSA(foursA, twos, twos, twosA, twosB);

    CSA(twosA, ones, ones, A[i+4], A[i+5]);
    CSA(twosB, ones, ones, A[i+6], A[i+7]);
    CSA(foursB, twos, twos, twosA, twosB);

    CSA(eights, fours, fours, foursA, foursB);

    tot += warrenBitCount(eights);
  }
  tot = 8*tot + 4*warrenBitCount(fours) + 2*warrenBitCount(twos) + warrenBitCount(ones);

  // Because I still don't fully trust this fancy version
  // assert(tot == wegnerCountBitsSetInMatrix(A, length));

  return (tot);
}

/*********************************************/
// Here are some timings made with quickTestMerge.c
// for the "5 5" case:

// Wegner CountBitsTime 29.3
// Warren CountBitsTime  5.3
// CSA    CountBitsTime  4.3
