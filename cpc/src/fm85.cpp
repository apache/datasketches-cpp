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

#include "common.h"
#include "fm85.h"
#include "fm85Compression.h"
#include "fm85Util.h"
#include "u32Table.h"

#include <stdexcept>
#include <new>

/*******************************************************/

U32 rowColFromTwoHashes (U64 hash0, U64 hash1, Short lgK) {
  if (lgK > 26) throw std::logic_error("lgK > 26");
  Long k = (1LL << lgK);
  Short col = countLeadingZerosInUnsignedLong (hash1); // 0 <= col <= 64
  if (col > 63) col = 63;                    // clip so that 0 <= col <= 63
  Long row = hash0 & (k - 1);
  U32 rowCol = (U32) ((row << 6) | col);
  // To avoid the hash table's "empty" value, we change the row of the following pair.
  // This case is extremely unlikely, but we might as well handle it.
  if (rowCol == ALL32BITS) { rowCol ^= (1 << 6); } 
  return (rowCol);
}

/*******************************************************/

Boolean fm85Initialized = 0;

// This is to support custom allocator and deallocator
void* (*fm85alloc)(size_t);
void (*fm85free)(void*);

// This stuff would be handled by Java's mechanism
// for initializing class variables.

void fm85Init (void) {
  fm85InitAD(&malloc, &free);
}

// This is to support custom allocator and deallocator
void fm85InitAD (void* (*alloc)(size_t), void (*dealloc)(void*)) {
  if (!fm85Initialized) {
    fm85Initialized = 1;
    fm85alloc = alloc;
    fm85free = dealloc;
    fillByteLeadingZerosTable();
    fillByteTrailingZerosTable();
    makeTheDecodingTables();
    fillInvPow2Tab ();
    fillKxpByteLookup ();
  }
}

void fm85Clean (void) {
  freeTheDecodingTables();
  fm85Initialized = 0;
}

/*******************************************************/
// The flavor is a function of K and C (the number of collected coupons).

enum flavorType determineFlavor (Short lgK, Long c) {
  Long k = (1LL << lgK);
  Long c2 = c << 1;
  Long c8 = c << 3;
  Long c32 = c << 5;
  if (c == 0)      return (EMPTY);    //    0  == C <    1
  if (c32 < 3*k)   return (SPARSE);   //    1  <= C <   3K/32
  if (c2 < k)      return (HYBRID);   // 3K/32 <= C <   K/2
  if (c8 < 27 * k) return (PINNED);   //   K/2 <= C < 27K/8
  else             return (SLIDING);  // 27K/8 <= C
}

// Note: the <= occurs with equality except SPARSE-vs-HYBRID for K = 2^4

enum flavorType determineSketchFlavor (FM85 * self) {
  return (determineFlavor(self->lgK, self->numCoupons));
}

/*******************************************************/

Short determineCorrectOffset (Short lgK, Long c) {
  Long k = (1LL << lgK);
  Long tmp = (c << 3) - 19 * k;        // 8C - 19K
  if (tmp < 0) return ((Short) 0);
  return ((Short) (tmp >> (lgK + 3))); // tmp / 8K
}

/*******************************************************/

FM85 * fm85Make (Short lgK) {
  if (lgK < 4 || lgK > 26) throw std::invalid_argument("lgK must be between 4 and 26");
  FM85 * self = (FM85 *) fm85alloc (sizeof(FM85));
  if (self == NULL) throw std::bad_alloc();
  self->lgK = lgK;
  self->isCompressed = 0;
  self->mergeFlag = 0;

  self->numCoupons = 0LL;

  self->windowOffset = 0;
  self->slidingWindow = (U8 *) NULL;
  self->surprisingValueTable = (u32Table *) NULL;

  self->numCompressedSurprisingValues = 0;
  self->compressedSurprisingValues = (U32 *) NULL;
  self->csvLength = 0;
  self->compressedWindow = (U32 *) NULL;
  self->cwLength = 0;
  
  self->firstInterestingColumn = 0;

  Long k = (1LL << self->lgK);

  self->kxp = ((double) k) * 1.0;
  self->hipEstAccum = 0.0;
  self->hipErrAccum = 0.0;

  return (self);
}

/*******************************************************/

FM85 * fm85Copy (FM85 * self) {
  if (self == NULL) throw std::invalid_argument("self is null");
  FM85 * newObj = (FM85 *) shallowCopy ((void *) self, sizeof(FM85));

  if (self->surprisingValueTable != NULL) {
    newObj->surprisingValueTable = u32TableCopy (self->surprisingValueTable);
  }
  if (self->slidingWindow != NULL) {
    Long k = (1LL << self->lgK);
    size_t theSize = k * sizeof(U8);
    newObj->slidingWindow = (U8 *) shallowCopy ((void *) self->slidingWindow, theSize);
  }
  if (self->compressedSurprisingValues != NULL) {
    size_t theSize = self->csvLength * sizeof(U32);
    newObj->compressedSurprisingValues = (U32 *) shallowCopy ((void *) self->compressedSurprisingValues, theSize);
  }
  if (self->compressedWindow != NULL) {
    size_t theSize = self->cwLength * sizeof(U32);
    newObj->compressedWindow = (U32 *) shallowCopy ((void *) self->compressedWindow, theSize);
  }

  return (newObj);
}

/*******************************************************/

void fm85Free (FM85 * self) {
  if (self != NULL) {
    if (self->surprisingValueTable != NULL) u32TableFree (self->surprisingValueTable);
    if (self->slidingWindow != NULL) fm85free (self->slidingWindow);
    if (self->compressedSurprisingValues != NULL) fm85free (self->compressedSurprisingValues);
    if (self->compressedWindow != NULL) fm85free (self->compressedWindow);
    fm85free (self);
  }
}

/*******************************************************/

// Warning: this is called in several places, including during the
// transitional moments during which sketch invariants involving
// flavor and offset are out of whack and in fact we are re-imposing
// them. Therefore it cannot rely on determineFlavor() or
// determineCorrectOffset(). Instead it interprets the low level data
// structures "as is".

// This produces a full-size k-by-64 bit matrix from any Live sketch.

U64 * bitMatrixOfSketch (FM85 * self) {
  if (self->isCompressed != 0) throw std::logic_error("isCompressed != 0");
  Long k = (1LL << self->lgK);
  Short offset = self->windowOffset;
  if (offset < 0 || offset > 56) throw std::logic_error("offset < 0 || offset > 56");
  Long i = 0;
  U64 * matrix = (U64 *) fm85alloc ((size_t) (k * sizeof(U64)));
  if (matrix == NULL) throw std::bad_alloc();

// Fill the matrix with default rows in which the "early zone" is filled with ones.
// This is essential for the routine's O(k) time cost (as opposed to O(C)).
  U64 defaultRow = (1ULL << offset) - 1;
  for (i = 0; i < k; i++) { matrix[i] = defaultRow; } 

  if (self->numCoupons == 0) { 
    return (matrix); // Returning a matrix of zeros rather than NULL.
  }

  U8 * window = self->slidingWindow;
  if (window != NULL) { // In other words, we are in window mode, not sparse mode.
    for (i = 0; i < k; i++) { // set the window bits, trusting the sketch's current offset.
      matrix[i] |= (((U64) window[i]) << offset);
    }
  }

  u32Table * table = self->surprisingValueTable;
  if (table == NULL) throw std::logic_error("table == NULL");
  U32 * slots = table->slots;
  Long numSlots = (1LL << table->lgSize); 
  for (i = 0; i < numSlots; i++) { 
    U32 rowCol = slots[i];
    if (rowCol != ALL32BITS) {
      Short col = (Short) (rowCol & 63);
      Long  row = (Long)  (rowCol >> 6);
      // Flip the specified matrix bit from its default value.
      // In the "early" zone the bit changes from 1 to 0.
      // In the "late" zone the bit changes from 0 to 1.
      matrix[row] ^= (1ULL << col); 
    }
  }

  return(matrix);
}

/*******************************************************/

void promoteEmptyToSparse (FM85 * self) {
  if (self->numCoupons != 0) throw std::logic_error("numCoupons != 0");
  if (self->surprisingValueTable != NULL) throw std::logic_error("surprisingValueTable != NULL");
  self->surprisingValueTable = u32TableMake (2, 6 + self->lgK);
}

/*******************************************************/
// In terms of flavor, this promotes SPARSE to HYBRID.

void promoteSparseToWindowed (FM85 * self) {
  Long k = (1LL << self->lgK);
  Long c32 = self->numCoupons << 5;
  if (!(c32 == 3 * k || (self->lgK == 4 && c32 > 3 * k))) throw std::logic_error("wrong c32");
  Long i;

  U8 * window = (U8 *) fm85alloc ((size_t) (k * sizeof(U8)));
  if (window == NULL) throw std::bad_alloc();
  bzero ((void *) window, (size_t) k); // zero the memory (because we will be OR'ing into it)

  u32Table * newTable = u32TableMake (2, 6 + self->lgK);

  u32Table * oldTable = self->surprisingValueTable;
  U32 * oldSlots = oldTable->slots;
  Long oldNumSlots = (1LL << oldTable->lgSize); 

  if (self->windowOffset != 0) throw std::logic_error("windowOffset != 0");

  for (i = 0; i < oldNumSlots; i++) { 
    U32 rowCol = oldSlots[i];
    if (rowCol != ALL32BITS) {
      Short col = (Short) (rowCol & 63);
      if (col < 8) {
        Long row = (Long) (rowCol >> 6);
        window[row] |= (1 << col);
      }
      else {
        // cannot use u32TableMustInsert(), because it doesn't provide for growth
        Boolean isNovel = u32TableMaybeInsert (newTable, rowCol);
        if (isNovel != 1) throw std::logic_error("isNovel != 1");
      }
    }
  }
  //  fprintf (stderr, "Number of surprising values dropped from %lld to %lld\n", oldTable->numItems, newTable->numItems);

  if (self->slidingWindow != NULL) throw std::logic_error("slidingWindow != NULL");
  self->slidingWindow = window;
  
  self->surprisingValueTable = newTable;
  u32TableFree (oldTable);
}

/*******************************************************/
// The KXP register is a double with roughly 50 bits of precision, but
// it might need roughly 90 bits to track the value with perfect accuracy.
// Therefore we recalculate KXP occasionally from the sketch's full bitmatrix
// so that it will reflect changes that were previously outside the mantissa.

void refreshKXP (FM85 * self, U64 * bitMatrix) {
  Long k = (1LL << self->lgK);
  Long i;
  Short j;

 // for improved numerical accuracy, we separately sum the bytes of the U64's
  double byteSums [8]; // allocating on the stack

  for (j = 0; j < 8; j++) { byteSums[j] = 0.0; }

  for (i = 0; i < k; i++) {
    U64 word = bitMatrix[i];
    for (j = 0; j < 8; j++) { 
      U8 byte = word & 0xff;
      byteSums[j] += kxpByteLookup[byte];
      word >>= 8;
    }
  }

  double total = 0.0;
  for (j = 7; j >= 0; j--) { // the reverse order is important
    double factor = invPow2Tab[8*j]; // pow (256.0, (-1.0 * ((double) j)));
    total += factor * byteSums[j];
  }

  //  fprintf (stderr, "%.3f\n", ((double) self->numCoupons) / k);
  //  fprintf (stderr, "%.19g\told value of KXP\n", self->kxp);
  //  fprintf (stderr, "%.19g\tnew value of KXP\n", total);
  //  fflush (stderr);

  self->kxp = total;

}


/*******************************************************/
// this moves the sliding window

void modifyOffset (FM85 * self, Short newOffset) {
  if (newOffset < 0 || newOffset > 56) throw std::logic_error("newOffset < 0 || newOffset > 56");
  if (newOffset != self->windowOffset + 1) throw std::logic_error("newOffset != windowOffset + 1");
  if (newOffset != determineCorrectOffset (self->lgK, self->numCoupons)) throw std::logic_error("newOffset is wrong");

  if (self->slidingWindow == NULL) throw std::logic_error("slidingWindow == NULL");
  if (self->surprisingValueTable == NULL) throw std::logic_error("surprisingValueTable == NULL");
  Long k = (1LL << self->lgK);

  // Construct the full-sized bit matrix that corresponds to the sketch
  U64 * bitMatrix = bitMatrixOfSketch (self);
  if (bitMatrix == NULL) throw std::logic_error("bitMatrix == NULL");

  // refresh the KXP register on every 8th window shift.
  if ((newOffset & 0x7) == 0) { refreshKXP (self, bitMatrix); }

  u32TableClear (self->surprisingValueTable); // the new number of surprises will be about the same

  u32Table * table = self->surprisingValueTable;
  U8 * window = self->slidingWindow;
  U64 maskForClearingWindow = (0xffULL << newOffset) ^ ALL64BITS;
  U64 maskForFlippingEarlyZone = (1ULL << newOffset) - 1;
  U64 allSurprisesORed = 0;
  Long i = 0;

  for (i = 0; i < k; i++) {
    U64 pattern = bitMatrix[i];
    window[i] = (U8) ((pattern >> newOffset) & 0xff);
    pattern &= maskForClearingWindow;
    // The following line converts surprising 0's to 1's in the "early zone", 
    // (and vice versa, which is essential for this procedure's O(k) time cost).
    pattern ^= maskForFlippingEarlyZone; 
    allSurprisesORed |= pattern; // a cheap way to recalculate firstInterestingColumn
    while (pattern != 0) {
      Short col = countTrailingZerosInUnsignedLong (pattern);
      pattern = pattern ^ (1ULL << col); // erase the 1.
      U32 rowCol = (i << 6) | col;
      Boolean isNovel = u32TableMaybeInsert (table, rowCol);
      if (isNovel != 1) throw std::logic_error("isNovel != 1");
    }
  }

  fm85free (bitMatrix);
  self->windowOffset = newOffset;

  self->firstInterestingColumn = countTrailingZerosInUnsignedLong (allSurprisesORed);
  if (self->firstInterestingColumn > newOffset) self->firstInterestingColumn = newOffset; // corner case
}


  //  fprintf (stderr, "Changed the offset from %d to %d because C = %lld;", 
  //	  (int) self->windowOffset, (int) newOffset, self->numCoupons); 
  //  fprintf (stderr, "\t[%d]\n", (int) self->firstInterestingColumn);
  // fflush (stderr);


/*******************************************************/
// Call this whenever a new coupon has been collected.

void updateHIP (FM85 * self, Short rowCol) {
  Long k = (1LL << self->lgK);
  Short col = (Short) (rowCol & 63);  
  double oneOverP = ((double) k) / self->kxp;
  self->hipEstAccum += oneOverP;
  self->hipErrAccum += ((oneOverP * oneOverP) - oneOverP);
  self->kxp -= invPow2Tab[col+1]; // notice the "+1"
}

/*******************************************************/

double getHIPEstimate (FM85 * self) {
  if (self->mergeFlag != 0) throw std::logic_error("tried to get HIP estimate of merged sketch");
  return (self->hipEstAccum);
}

/*******************************************************/

void updateSparse (FM85 * self, U32 rowCol) {
  Long k = (1LL << self->lgK);
  Long c32pre = self->numCoupons << 5;
  if (c32pre >= 3*k) throw std::logic_error("c32pre >= 3*k"); // C < 3K/32, in other words flavor == SPARSE
  if (self->surprisingValueTable == NULL) throw std::logic_error("surprisingValueTable == NULL");
  Boolean isNovel = u32TableMaybeInsert (self->surprisingValueTable, rowCol);
  if (isNovel) {
    self->numCoupons += 1;
    updateHIP (self, rowCol);
    Long c32post = self->numCoupons << 5;
    if (c32post >= 3*k) { promoteSparseToWindowed (self); } // C >= 3K/32
  }
}

/*******************************************************/

// the flavor is HYBRID, PINNED, or SLIDING.
void updateWindowed (FM85 * self, U32 rowCol) {
  if (self->windowOffset < 0 || self->windowOffset > 56) throw std::logic_error("windowOffset < 0 || windowOffset > 56");
  Long k = (1LL << self->lgK);
  Long c32pre = self->numCoupons << 5;
  if (c32pre < 3*k) throw std::logic_error("c32pre < 3*k"); // C < 3K/32, in other words flavor >= HYBRID
  Long c8pre = self->numCoupons << 3;
  Long w8pre = ((Long) self->windowOffset) << 3;
  if (c8pre >= (27 + w8pre) * k) throw std::logic_error("c8pre is wrong"); // C < (K * 27/8) + (K * windowOffset)

  Boolean isNovel = 0;
  Short col = (Short) (rowCol & 63);

  if (col < self->windowOffset) { // track the surprising 0's "before" the window
    isNovel = u32TableMaybeDelete (self->surprisingValueTable, rowCol); // inverted logic
  }
  else if (col < self->windowOffset + 8) { // track the 8 bits inside the window
    if (col < self->windowOffset) throw std::logic_error("col < windowOffset");
    Long row = (Long) (rowCol >> 6);
    U8 oldBits = self->slidingWindow[row];
    U8 newBits = oldBits | (1 << (col - self->windowOffset));
    if (newBits != oldBits) {
      self->slidingWindow[row] = newBits;      
      isNovel = 1;
    }
  }
  else { // track the surprising 1's "after" the window
    if (col < self->windowOffset + 8) throw std::logic_error("col < windowOffset + 8");
    isNovel = u32TableMaybeInsert (self->surprisingValueTable, rowCol); // normal logic
  }

  if (isNovel) {
    self->numCoupons += 1;
    updateHIP (self, rowCol);
    Long c8post = self->numCoupons << 3;
    if (c8post >= (27 + w8pre) * k) { 
      modifyOffset (self, self->windowOffset + 1);
      if (self->windowOffset < 1 || self->windowOffset > 56) throw std::logic_error("windowOffset < 1 || windowOffset > 56");
      Long w8post = ((Long) self->windowOffset) << 3;
      if (c8post >= (27 + w8post) * k) throw std::logic_error("c8pre is wrong"); // C < (K * 27/8) + (K * windowOffset)
    }
  }
}

/*******************************************************/

void fm85RowColUpdate (FM85 * self, U32 rowCol) {
  Short col = (Short) (rowCol & 63);
  if (col < self->firstInterestingColumn) { return; } // important speed optimization
  if (self->isCompressed) std::logic_error("Cannot update a compressed sketch");
  Long c = self->numCoupons;
  if (c == 0) { promoteEmptyToSparse (self); }
  Long k = (1LL << self->lgK);
  if ((c << 5) < 3*k) { updateSparse (self, rowCol); }
  else { updateWindowed (self, rowCol); }
}


void fm85Update (FM85 * self, U64 hash0, U64 hash1) {
  U32 rowCol = rowColFromTwoHashes (hash0, hash1, self->lgK);
  fm85RowColUpdate (self, rowCol);
}

