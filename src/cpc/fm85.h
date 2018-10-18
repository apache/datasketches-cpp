/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

// author Kevin Lang, Oath Research

#ifndef GOT_FM85_H

#include "common.h"

#include "u32Table.h"

// Note: except for brief transitional moments, these sketches always obey
// the following strict mapping between the flavor of a sketch and the
// number of coupons that it has collected.

enum flavorType {
  EMPTY,   //    0  == C <    1
  SPARSE,  //    1  <= C <   3K/32
  HYBRID,  //  3K/32 <= C <   K/2
  PINNED,  //   K/2 <= C < 27K/8  [NB: 27/8 = 3 + 3/8]
  SLIDING  // 27K/8 <= C
};

/*******************************************************/

typedef struct fm85_sketch_type
{
  // The following variables occur in all sketch types.
  Short lgK;
  Boolean isCompressed;
  Boolean mergeFlag; // Is the sketch the result of merging?

  Long numCoupons; // The number of coupons collected so far.

  // The following variables occur in the updateable semi-compressed type.
  U8 * slidingWindow;
  Short windowOffset; // Derivable from numCoupons, but made explicit for speed.
  u32Table * surprisingValueTable;

  // The following variables occur in the non-updateable fully-compressed type.
  U32 * compressedWindow; // A bitstream.
  Long  cwLength; // The number of 32-bit words in this bitstream. (Not needed in Java).
  Long  numCompressedSurprisingValues;
  U32 * compressedSurprisingValues; // A bitstream.
  Long  csvLength; // The number of 32-bit words in this bitstream. (Not needed in Java).

  // Note that (as an optimization) the two bitstreams could be concatenated.

  Short firstInterestingColumn; // This is part of a speed optimization.

  double kxp;
  double hipEstAccum;
  double hipErrAccum;

} FM85;

/*******************************************************/
// These routines are exported.

void fm85Init (void); // Call this before anything else.

FM85 * fm85Make (Short lgK);

FM85 * fm85Copy (FM85 * self);

void fm85Free (FM85 * sketch);

void fm85Update (FM85 * sketch, U64 hash0, U64 hash1);

double getHIPEstimate (FM85 * sketch);

// getIconEstimate() is defined in a separate file.

/*******************************************************/

// The following is used during testing, and is basically package private.
U32 rowColFromTwoHashes (U64 hash0, U64 hash1, Short lgK);

/*******************************************************/
// These routines are internal.

void fm85RowColUpdate (FM85 * sketch, U32 rowCol);

enum flavorType determineFlavor (Short lgK, Long c);
enum flavorType determineSketchFlavor (FM85 * self);

Short determineCorrectOffset (Short lgK, Long c);

U64 * bitMatrixOfSketch (FM85 * self);

// these are only used internally
// void promoteEmptyToSparse (FM85 * self);
// void promoteSparseToWindowed (FM85 * self);
// void modifyOffset (FM85 * self, Short newOffset);
// void updateSparse   (FM85 * self, U32 rowCol);
// void updateWindowed (FM85 * self, U32 rowCol);


/*******************************************************/

#define GOT_FM85_H
#endif

