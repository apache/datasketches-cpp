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

#ifndef GOT_FM85_MERGING_H

#include "common.h"
#include "fm85.h"
#include "fm85Compression.h"
#include "fm85Util.h"
#include "u32Table.h"

/****************************************/

typedef struct fm85_unioning_gadget
{
  Short lgK; // Note: in some cases this will be reduced.
  FM85 * accumulator; // this is a sketch object
  U64  * bitMatrix;
  // Note: at most one of the previous two fields will be non-NULL at any given moment.
  // accumulator is a sketch object that is employed until it graduates out of Sparse mode.
  // At that point, it is converted into a full-sized bitMatrix, which is mathematically a sketch,
  // but doesn't maintain any of the "extra" fields of our sketch objects, so some additional work
  // is required when getResult is called at the end.
} UG85;

/****************************************/

UG85 * ug85Make (Short lgK);

void ug85Free (UG85 * unioner);

void ug85MergeInto (UG85 * unioner, FM85 * sourceSketch);

FM85 * ug85GetResult (UG85 * unioner);

/****************************************/

U64 * bitMatrixOfUG85 (UG85 * self, Boolean * needToFreePtr); // used for testing

/****************************************/

#define GOT_FM85_MERGING_H
#endif

// The merging logic is somewhat involved, so it will be summarized here.

//  First, we compare the K values of the unioner and the source sketch.

//  if source.K < unioner.K, we reduce the unioner's K to match, which
//  requires downsampling the unioner's internal sketch. 

//  Here is how to perform the downsampling.
//  If the unioner contains a bitMatrix, downsample it by row-wise OR'ing.
//  If the unioner contains a sparse sketch, then create a new empty
//  sketch, and walk the old target sketch updating the new one (with modulo).
//  At the end, check whether the new target
//  sketch is still in sparse mode (it might not be, because downsampling
//  densifies the set of collected coupons). If it is NOT in sparse mode, 
//  immediately convert it to a bitmatrix.

// At this point, we have source.K >= unioner.K.
// [We won't keep mentioning this, but in all of the following the
// source's row indices are used mod unioner.K while updating the unioner's sketch.
// That takes care of the situation where source.K > unioner.K.]

// Case A: unioner is Sparse and source is Sparse. We walk the source sketch
// updating the unioner's sketch. At the end, if the unioner's sketch
// is no longer in sparse mode, we convert it to a bitmatrix.

// Case B: unioner is bitmatrix and source is Sparse. We walk the source sketch,
// setting bits in the bitmatrix.

// In the remaining cases, we have flavor(source) > Sparse, so we immediately convert the
// unioner's sketch to a bitmatrix (even if the unioner contains very few coupons). Then:

// Case C: unioner is bitmatrix and source is Hybrid or Pinned. Then we OR the source's
// sliding window into the bitmatrix, and walk the source's table, setting bits in the bitmatrix.

// Case D: unioner is bitmatrix, and source is Sliding. Then we convert the source into
// a bitmatrix, and OR it into the unioner's bitmatrix. [Important note; merely walking the source
// wouldn't work because of the partially inverted Logic in the Sliding flavor, where the presence of
// coupons is sometimes indicated by the ABSENCE of rowCol pairs in the surprises table.]

/****************************************/

// How does getResult work?

// If the unioner is using its accumator field, make a copy of that sketch.

// If the unioner is using its bitMatrix field, then we have to convert the
// bitMatrix back into a sketch, which requires doing some extra work to
// figure out the values of numCoupons, offset, firstInterestingColumn, and KxQ.

/*******************************************************************************************/
