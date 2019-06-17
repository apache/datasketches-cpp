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

#include "fm85Merging.h"

#include <stdexcept>
#include <new>

UG85 * ug85Make (Short lgK) {
  if (lgK < 4) throw std::invalid_argument("lgK < 4");
  UG85 * self = (UG85 *) fm85alloc (sizeof(UG85));
  if (self == NULL) throw std::bad_alloc();
  self->lgK = lgK;
  // We begin with the accumulator holding an EMPTY sketch object.
  // As an optimization the accumulator could start as NULL, but that would require changes elsewhere.
  self->accumulator = fm85Make (lgK);
  self->bitMatrix = NULL;
  return (self);
}

/*******************************************************************************************/

void ug85Free (UG85 * self) {
  if (self != NULL) {
    if (self->accumulator != NULL) { fm85Free (self->accumulator); }
    if (self->bitMatrix != NULL) { fm85free (self->bitMatrix); }
    fm85free (self);
  }
}

/*******************************************************************************************/

// This is used for testing purposes only.
U64 * bitMatrixOfUG85 (UG85 * self, Boolean * needToFreePtr) {
  if (self->bitMatrix != NULL) { // return the matrix
    if (self->accumulator != NULL) throw std::logic_error("accumulator is not null");
    *needToFreePtr = 0;  // or we could make a copy of the bitmatrix
    return (self->bitMatrix);
  }
  else { // construct a matrix
    if (self->accumulator == NULL) throw std::logic_error("accumulator is null");
    *needToFreePtr = 1;
    return (bitMatrixOfSketch (self->accumulator));
  }
}

/*******************************************************************************************/
/*******************************************************************************************/

void walkTableUpdatingSketch (FM85 * dest, u32Table * table) {
  U32 * slots = table->slots;
  Long numSlots = (1LL << table->lgSize); 
  if (dest->lgK > 26) throw std::logic_error("dest->lgK > 26");
  U32 destMask = (((1 << dest->lgK) - 1) << 6) | 63;  // downsamples when destlgK < srcLgK

  // Using a golden ratio stride fixes the snowplow effect.
  double golden = 0.6180339887498949025;
  Long stride = (Long) (golden * ((double) numSlots));
  if (stride < 2) throw std::logic_error("stride < 2");
  if (stride == ((stride >> 1) << 1)) { stride += 1; }; // force the stride to be odd
  if (stride < 3 || stride >= numSlots) throw std::out_of_range("stride out of range");

  Long i,j;
  for (i = 0, j = 0; i < numSlots; i++, j += stride) {
    j &= (numSlots - 1LL);
    U32 rowCol = slots[j];
    if (rowCol != ALL32BITS) {
      fm85RowColUpdate (dest, rowCol & destMask); 
    }
  }
}

/*******************************************************************************************/

void orTableIntoMatrix (U64 * bitMatrix, Short destLgK, u32Table * table) {
  U32 * slots = table->slots;
  Long numSlots = (1LL << table->lgSize); 
  Long destMask = (1LL << destLgK) - 1LL;  // downsamples when destlgK < srcLgK
  Long i = 0;
  for (i = 0; i < numSlots; i++) { 
    U32 rowCol = slots[i];
    if (rowCol != ALL32BITS) {
      Short col = (Short) (rowCol & 63);
      Long  row = (Long)  (rowCol >> 6);
      bitMatrix[row & destMask] |= (1ULL << col); // Set the bit.
    }
  }
}

/*******************************************************************************************/

void orWindowIntoMatrix (U64 * destMatrix, Short destLgK, U8 * srcWindow, Short srcOffset, Short srcLgK) {
  if (destLgK > srcLgK) throw std::logic_error("destLgK > srcLgK");
  Long destMask = (1LL << destLgK) - 1LL;  // downsamples when destlgK < srcLgK
  Long srcK = (1LL << srcLgK);
  Long srcRow = 0;
  for (srcRow = 0; srcRow < srcK; srcRow++) {
    destMatrix[srcRow & destMask] |= (((U64) srcWindow[srcRow]) << srcOffset);
  }
}

/*******************************************************************************************/

void orMatrixIntoMatrix (U64 * destMatrix, Short destLgK, U64 * srcMatrix, Short srcLgK) {
  if (destLgK > srcLgK) throw std::logic_error("destLgK > srcLgK");
  Long destMask = (1LL << destLgK) - 1LL; // downsamples when destlgK < srcLgK
  Long srcK = (1LL << srcLgK);
  Long srcRow = 0;
  for (srcRow = 0; srcRow < srcK; srcRow++) {
    destMatrix[srcRow & destMask] |= srcMatrix[srcRow];
  }
}

/*******************************************************************************************/

void ug85ReduceK (UG85 * unioner, Short newLgK) {
  if (newLgK >= unioner->lgK) throw std::logic_error("newLgK >= unioner->lgK");
  if (unioner->accumulator == NULL && unioner->bitMatrix == NULL) throw std::logic_error("both accumulator and bitMatrix are null");

  if (unioner->bitMatrix != NULL) { // downsample the unioner's bit matrix
    if (unioner->accumulator != NULL) throw std::logic_error("accumulator is not null");
    Long newK = (1LL << newLgK);
    U64 * newMatrix = (U64 *) fm85alloc ((size_t) (newK * sizeof(U64)));
    if (newMatrix == NULL) throw std::bad_alloc();
    Long i = 0;
    for (i = 0; i < newK; i++) { newMatrix[i] = 0LL; } // clear the bit matrix
    orMatrixIntoMatrix (newMatrix, newLgK, unioner->bitMatrix, unioner->lgK);
    fm85free (unioner->bitMatrix);
    unioner->bitMatrix = newMatrix;
    unioner->lgK = newLgK;
    return;
  }

  if (unioner->accumulator != NULL) { // downsample the unioner's sketch
    if (unioner->bitMatrix != NULL) throw std::logic_error("bitMatrix is not null");
    FM85 * oldSketch = unioner->accumulator;

    if (oldSketch->numCoupons == 0) { // if the accumulator is EMPTY, simply change its K.
      if (oldSketch->surprisingValueTable != NULL) throw std::logic_error("oldSketch->surprisingValueTable is not null");
      oldSketch->lgK = newLgK;
      unioner->lgK = newLgK;
      return;
    }

    FM85 * newSketch = fm85Make (newLgK);
    if (oldSketch->slidingWindow != NULL || oldSketch->surprisingValueTable == NULL) throw std::logic_error("invalid state");
    walkTableUpdatingSketch (newSketch, oldSketch->surprisingValueTable);

    enum flavorType finalNewFlavor = determineSketchFlavor(newSketch);
    if (finalNewFlavor == EMPTY) throw std::logic_error("finalNewFlavor == EMPTY");
    if (finalNewFlavor == SPARSE) {
      unioner->accumulator = newSketch;
      unioner->lgK = newLgK;
      fm85Free (oldSketch);
      return;
    }
    else { // the new sketch has graduated beyond sparse, so convert to bitMatrix
      unioner->accumulator = NULL;
      unioner->bitMatrix = bitMatrixOfSketch (newSketch);
      unioner->lgK = newLgK;
      fm85Free (oldSketch);
      fm85Free (newSketch);
      return;
    }
  }

  throw std::logic_error("invalid state");
}

/*******************************************************************************************/

void ug85MergeInto (UG85 * unioner, FM85 * source) {
  if (NULL == unioner) throw std::invalid_argument("unioner is null");
  if (NULL == source) return;
  
  enum flavorType sourceFlavor = determineSketchFlavor(source);
  if (EMPTY == sourceFlavor) return;

  if (source->lgK < unioner->lgK) { ug85ReduceK (unioner, source->lgK); }

  if (source->lgK < unioner->lgK) throw std::logic_error("source->lgK < unioner->lgK");

  if (unioner->accumulator == NULL && unioner->bitMatrix == NULL) throw std::logic_error("both accumulator and bitMatrix are null");

  if (SPARSE == sourceFlavor && unioner->accumulator != NULL)  { // Case A
    if (unioner->bitMatrix != NULL) throw std::logic_error("unioner->bitMatrix != NULL");
    enum flavorType initialDestFlavor = determineSketchFlavor (unioner->accumulator);
    if (EMPTY != initialDestFlavor && SPARSE != initialDestFlavor) throw std::logic_error("wrong flavor");

    // The following partially fixes the snowplow problem provided that the K's are equal.
    // A complete fix is coming soon.
    if (EMPTY == initialDestFlavor && unioner->lgK == source->lgK) { 
      fm85Free (unioner->accumulator);      
      unioner->accumulator = fm85Copy(source);
    }

    walkTableUpdatingSketch (unioner->accumulator, source->surprisingValueTable);
    enum flavorType finalDestFlavor = determineSketchFlavor(unioner->accumulator);
    // if the accumulator has graduated beyond sparse, switch to a bitMatrix representation
    if (finalDestFlavor != EMPTY && finalDestFlavor != SPARSE) {
      unioner->bitMatrix = bitMatrixOfSketch (unioner->accumulator);
      fm85Free (unioner->accumulator);
      unioner->accumulator = NULL;
    }
    return;
  }

  if (SPARSE == sourceFlavor && unioner->bitMatrix != NULL)  { // Case B
    if (unioner->accumulator != NULL) throw std::logic_error("unioner->accumulator != NULL");
    orTableIntoMatrix (unioner->bitMatrix, unioner->lgK, source->surprisingValueTable);
    return;
  }

  if (HYBRID != sourceFlavor && PINNED != sourceFlavor && SLIDING != sourceFlavor) throw std::logic_error("wrong flavor");

 // source is past SPARSE mode, so make sure that dest is a bitMatrix.
  if (unioner->accumulator != NULL) {
    if (unioner->bitMatrix != NULL) throw std::logic_error("unioner->bitMatrix != NULL");
    enum flavorType destFlavor = determineSketchFlavor (unioner->accumulator);
    if (EMPTY != destFlavor && SPARSE != destFlavor) throw std::logic_error("wrong flavor");
    unioner->bitMatrix = bitMatrixOfSketch (unioner->accumulator);
    fm85Free (unioner->accumulator);
    unioner->accumulator = NULL;
  }
  if (unioner->bitMatrix == NULL) throw std::logic_error("unioner->bitMatrix == NULL");

  if (HYBRID == sourceFlavor || PINNED == sourceFlavor) { // Case C
    orWindowIntoMatrix (unioner->bitMatrix, unioner->lgK, source->slidingWindow, source->windowOffset, source->lgK);
    orTableIntoMatrix (unioner->bitMatrix, unioner->lgK, source->surprisingValueTable);
    return;
  }
  
  // SLIDING mode involves inverted logic, so we can't just walk the source sketch.
  // Instead, we convert it to a bitMatrix that can be OR'ed into the destination.
  if (SLIDING != sourceFlavor) throw std::logic_error("wrong flavor"); // Case D
  U64 * sourceMatrix = bitMatrixOfSketch (source);
  orMatrixIntoMatrix (unioner->bitMatrix, unioner->lgK, sourceMatrix, source->lgK);
  fm85free (sourceMatrix);

  return;
}

/*******************************************************************************************/

FM85 * ug85GetResult (UG85 * unioner) {
  if (unioner == NULL) throw std::invalid_argument("unioner == NULL");
  if (unioner->accumulator == NULL && unioner->bitMatrix == NULL) throw std::logic_error("both accumulator and bitMatrix are null");

  if (unioner->accumulator != NULL) { // start of case where unioner contains a sketch
    if (unioner->bitMatrix != NULL) throw std::logic_error("unioner->bitMatrix != NULL");
    if (unioner->lgK != unioner->accumulator->lgK) throw std::logic_error("unioner->lgK != unioner->accumulator->lgK");
    if (unioner->accumulator->numCoupons == 0) {
      FM85 * result = fm85Make (unioner->lgK);
      result->mergeFlag = 1;
      return (result);
    }
    if (SPARSE != determineSketchFlavor(unioner->accumulator)) throw std::logic_error("wrong flavor");
    FM85 * result = fm85Copy (unioner->accumulator);
    result->mergeFlag = 1;
    return (result);
  } // end of case where unioner contains a sketch

  // start of case where unioner contains a bitMatrix
  if (unioner->bitMatrix == NULL) throw std::logic_error("unioner->bitMatrix == NULL");
  if (unioner->accumulator != NULL) throw std::logic_error("unioner->accumulator != NULL");
  U64 * matrix = unioner->bitMatrix;
  Short lgK = unioner->lgK;
  FM85 * result = fm85Make (unioner->lgK); 

  Long k = (1LL << lgK);
  Long numCoupons = countBitsSetInMatrix (matrix, k);
  result->numCoupons = numCoupons;

  enum flavorType flavor = determineFlavor (lgK, numCoupons);
  if (flavor != HYBRID && flavor != PINNED && flavor != SLIDING) throw std::logic_error("wrong flavor");

  Short offset = determineCorrectOffset (lgK, numCoupons);
  result->windowOffset = offset;

  U8 * window = (U8 *) fm85alloc ((size_t) (k * sizeof(U8)));
  if (window == NULL) throw std::bad_alloc();
  // don't need to zero the window's memory
  if (result->slidingWindow != NULL) throw std::logic_error("result->slidingWindow != NULL");
  result->slidingWindow = window;

  //  u32Table * table = u32TableMake (2, 6 + lgK); // dynamically growing caused snowplow effect
  Short newTableSize = lgK - 4; //   K/16; in some cases this will end up being oversized
  if (newTableSize < 2) newTableSize = 2;
  u32Table * table = u32TableMake (newTableSize, 6 + lgK); 
  if (result->surprisingValueTable != NULL) throw std::logic_error("result->surprisingValueTable != NULL");
  result->surprisingValueTable = table;

  // I believe that the following works even when the offset is zero.
  U64 maskForClearingWindow = (0xffULL << offset) ^ ALL64BITS;
  U64 maskForFlippingEarlyZone = (1ULL << offset) - 1;
  U64 allSurprisesORed = 0;
  Long i = 0;

  // The snowplow effect was caused by processing the rows in order,
  // but we have fixed it by using a sufficiently large hash table.
  for (i = 0; i < k; i++) {
    U64 pattern = matrix[i];
    window[i] = (U8) ((pattern >> offset) & 0xff);
    pattern &= maskForClearingWindow;
    pattern ^= maskForFlippingEarlyZone; // This flipping converts surprising 0's to 1's.
    allSurprisesORed |= pattern;
    while (pattern != 0) {
      Short col = countTrailingZerosInUnsignedLong (pattern);
      pattern = pattern ^ (1ULL << col); // erase the 1.
      U32 rowCol = (i << 6) | col;
      Boolean isNovel = u32TableMaybeInsert (table, rowCol);
      if (isNovel != 1) throw std::logic_error("isNovel != 1");
    }
  }

  // At this point we could shrink an oversized hash table, but the relative waste isn't very big.

  result->firstInterestingColumn = countTrailingZerosInUnsignedLong (allSurprisesORed);
  if (result->firstInterestingColumn > offset) result->firstInterestingColumn = offset; // corner case

  // NB: the HIP-related fields will contain bogus values, but that is okay.

  result->mergeFlag = 1;
  return result;
  // end of case where unioner contains a bitMatrix
}
