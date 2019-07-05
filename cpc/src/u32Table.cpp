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

#include "u32Table.h"
#include "common.h"
#include "fm85Util.h"

#include <stdexcept>
#include <new>

extern void* (*fm85alloc)(size_t);
extern void (*fm85free)(void*);

/*******************************************************/

u32Table * u32TableMake (Short lgSize, Short numValidBits) {
  if (lgSize < 2) throw std::invalid_argument("lgSize must be >= 2");
  Long numSlots = (1LL << lgSize);
  u32Table * self = (u32Table *) fm85alloc (sizeof(u32Table));
  if (self == NULL) throw std::bad_alloc();
  U32 * arr = (U32 *) fm85alloc ((size_t) (numSlots * sizeof(U32)));
  if (arr == NULL) throw std::bad_alloc();
  Long i = 0;
  for (i = 0; i < numSlots; i++) { arr[i] = ALL32BITS; }
  if (numValidBits < 1 || numValidBits > 32) throw std::invalid_argument("numValidBits must be between 1 and 32");
  self->validBits = numValidBits;
  self->lgSize = lgSize;
  self->numItems = 0;
  self->slots = arr;
  return (self);
}

/*******************************************************/

u32Table * u32TableCopy (u32Table * self) {
  if (self == NULL) throw std::invalid_argument("self is null");
  if (self->slots == NULL) throw std::invalid_argument("no slots");
  Long numSlots = (1LL << self->lgSize);
  u32Table * newObj = (u32Table *) shallowCopy ((void *) self, sizeof(u32Table));
  newObj->slots = (U32 *) shallowCopy ((void *) self->slots, ((size_t) numSlots) * sizeof(U32));
  return (newObj);
}

//  newObj->validBits = self->validBits;
//  newObj->lgSize    = self->lgSize;
//  newObj->numItems  = self->numItems;

/*******************************************************/

void u32TableFree (u32Table * self) {
  if (self != NULL) {
    if (self->slots != NULL) fm85free (self->slots);
    fm85free (self);
  }
}

/*******************************************************/

void u32TableShow (u32Table * self) {
  Long tableSize = 1LL << self->lgSize;
  printf ("\nu32Table (%d valid bits; %lld of %lld slots occupied)\n",
	  self->validBits, (long long int) self->numItems, (long long int) tableSize);
  //  U32 * arr = self->slots;
  //  Long i;
  //  for (i = 0; i < tableSize; i++) {
  //    if (arr[i] == ALL32BITS) printf ("%d:\tempty\n", (int) i);
  //    else printf ("%d:\t%8X\n", (int) i, arr[i]);
  //  }
  fflush (stdout);
}

/*******************************************************/

void u32TableClear (u32Table * self) { // clear the table without resizing it
  Long tableSize = 1LL << self->lgSize;
  U32 * arr = self->slots;
  Long i;
  for (i = 0; i < tableSize; i++) { arr[i] = ALL32BITS; }
  self->numItems = 0;
}

/*******************************************************/

void printU32Array (U32 * array, Long arrayLength) {
  Long i = 0;
  printf ("\nu32Array [%lld]\n", (long long int) arrayLength);
  for (i = 0; i < arrayLength; i++) {
    printf ("%d:\t%8X\n", (int) i, array[i]);    
  }
  fflush (stdout);
}

/*******************************************************/

#define U32_TABLE_LOOKUP_SHARED_CODE_SECTION \
  Long tableSize = 1LL << self->lgSize; \
  Long mask = tableSize - 1LL; \
  Short shift = self->validBits - self->lgSize; \
  Long probe = ((Long) item) >> shift; \
  if (probe < 0 || probe > mask) throw std::out_of_range("probe out of range"); \
  U32 * arr = self->slots; \
  U32 fetched = arr[probe]; \
  while (fetched != item && fetched != ALL32BITS) { \
    probe = (probe + 1) & mask; \
    fetched = arr[probe]; \
  }

/*******************************************************/

void u32TableMustInsert (u32Table * self, U32 item) {
  U32_TABLE_LOOKUP_SHARED_CODE_SECTION;
  if (fetched == item) { throw std::logic_error("item exists"); }
  else {
    if (fetched != ALL32BITS) throw std::logic_error("could not insert");
    arr[probe] = item;
    // counts and resizing must be handled by the caller.
  }
}

/*******************************************************/

// This one is specifically tailored to be part of our fm85 decompression scheme.

u32Table * makeU32TableFromPairsArray (U32 * pairs, Long numPairs, Short sketchLgK) {
  Short lgNumSlots = 2;
  while (u32TableUpsizeDenom * numPairs > u32TableUpsizeNumer * (1LL << lgNumSlots)) { lgNumSlots++; }
  u32Table * table = u32TableMake (lgNumSlots, 6 + sketchLgK); // Already filled with the "Empty" value which is ALL32BITS.
  Long i = 0;
  // Note: there is a possible "snowplow effect" here because the caller is passing in a sorted pairs array.
  // However, we are starting out with the correct final table size, so the problem might not occur.
  for (i = 0; i < numPairs; i++) {
    u32TableMustInsert (table, pairs[i]);
  }
  table->numItems = numPairs;
  return (table);
}

/*******************************************************/

void privateU32TableRebuild (u32Table * self, Short newLgSize) {
  if (newLgSize < 2) throw std::logic_error("newLgSize < 2");
  Long newSize = (1LL << newLgSize);
  Long oldSize = (1LL << self->lgSize);
  //  printf ("rebuilding: %lld -> %lld; %lld items in table\n", oldSize, newSize, self->numItems); fflush (stdout);
  if (newSize <= self->numItems) throw std::logic_error("newSize <= numItems");
  U32 * oldSlots = self->slots;
  U32 * newSlots = (U32 *) fm85alloc ((size_t) (newSize * sizeof(U32)));
  if (newSlots == NULL) throw std::bad_alloc();
  Long i;
  for (i = 0; i < newSize; i++) { 
    newSlots[i] = ALL32BITS;
  }  
  self->slots = newSlots;
  self->lgSize = newLgSize;
  for (i = 0; i < oldSize; i++) {
    U32 item = oldSlots[i];
    if (item != ALL32BITS) {
      u32TableMustInsert (self, item);
    }
  }
  fm85free (oldSlots);
  return;
}

/*******************************************************/

// Returns true iff the item was new and was therefore added to the table.

Boolean u32TableMaybeInsert (u32Table * self, U32 item) {
  U32_TABLE_LOOKUP_SHARED_CODE_SECTION;
  if (fetched == item) { return 0; }
  else {
    if (fetched != ALL32BITS) throw std::logic_error("could not insert");
    arr[probe] = item;
    self->numItems += 1;
    while (u32TableUpsizeDenom * self->numItems > u32TableUpsizeNumer * (1LL << self->lgSize)) {
      privateU32TableRebuild(self, self->lgSize + 1);
    }
    return 1;
  }
}

/*******************************************************/

// Returns true iff the item was present and was therefore removed from the table.

Boolean u32TableMaybeDelete (u32Table * self, U32 item) {
  U32_TABLE_LOOKUP_SHARED_CODE_SECTION;
  if (fetched == ALL32BITS) { return 0; }
  else {
    if (fetched != item) throw std::logic_error("item does not exist");
    // delete the item
    arr[probe] = ALL32BITS;
    self->numItems -= 1;
    if (self->numItems < 0) throw std::logic_error("delete error");

    // re-insert all items between the freed slot and the next empty slot
    probe = (probe + 1) & mask; fetched = arr[probe];
    while (fetched != ALL32BITS) {
      arr[probe] = ALL32BITS;
      u32TableMustInsert (self, fetched);
      probe = (probe + 1) & mask; fetched = arr[probe];      
    }

    // shrink if necessary
    while (u32TableDownsizeDenom * self->numItems < u32TableDownsizeNumer * (1LL << self->lgSize) && self->lgSize > 2) {
      privateU32TableRebuild(self, self->lgSize - 1);
    }
    return 1;
  }
}

/*******************************************************/

// While extracting the items from a linear probing hashtable,
// this will usually undo the wrap-around provided that the table 
// isn't too full. Experiments suggest that for sufficiently large tables 
// the load factor would have to be over 90 percent before this would fail frequently, 
// and even then the subsequent sort would fix things up.

U32 * u32TableUnwrappingGetItems (u32Table * self, Long * returnNumItems) {
  *returnNumItems = self->numItems;
  if (self->numItems < 1) { return (NULL); }
  U32 * slots = self->slots;
  Long tableSize = (1LL << self->lgSize);
  U32 * result = (U32 *) fm85alloc ((size_t) (self->numItems * sizeof(U32)));
  if (result == NULL) throw std::bad_alloc();
  Long i = 0;
  Long l = 0;
  Long r = self->numItems - 1;

  // Special rules for the region before the first empty slot.
  U32 hiBit = 1 << (self->validBits - 1);
  while (i < tableSize && slots[i] != ALL32BITS) {
    U32 item = slots[i++];
    if (item & hiBit) { result[r--] = item; } // This item was probably wrapped, so move to end.
    else              { result[l++] = item; }
  }

  // The rest of the table is processed normally.
  while (i < tableSize) {
    U32 look = slots[i++];
    if (look != ALL32BITS) { result[l++] = look; }
  }
  if (l != r + 1) throw std::logic_error("unwrapping error");
  return (result);
}


/*******************************************************/
// The Java version won't need this, because it provides a good array sort.

void u32KnuthShellSort3(U32 a[], Long l, Long r)
{ Long i, h;
  for (h = 1; h <= (r-l)/9; h = 3*h+1) ;
  for ( ; h > 0; h /= 3) {
    for (i = l+h; i <= r; i++) {
      Long j = i; U32 v = a[i]; 
      while (j >= l+h && v < a[j-h])
	{ a[j] = a[j-h]; j -= h; }
      a[j] = v; 
    } 
  }
  Long bad = 0;
  for (i = l; i < r-1; i++) {
    if (a[i] > a[i+1]) bad++;
  };
  if (bad != 0) throw std::logic_error("sorting error");
}

/*******************************************************/
// In applications where the input array is already nearly sorted,
// insertion sort runs in linear time with a very small constant.
// This introspective version of insertion sort protects against
// the quadratic cost of sorting bad input arrays.
// It keeps track of how much work has been done, and if that exceeds a
// constant times the array length, it switches to a different sorting algorithm.

void introspectiveInsertionSort(U32 a[], Long l, Long r) // r points AT the rightmost element
{ Long i;
  Long length = r - l + 1;
  Long cost = 0;
  Long costLimit = 8 * length;
  for (i = l+1; i <= r; i++) {
    Long j = i; 
    U32 v = a[i]; 
    while (j >= l+1 && v < a[j-1]) { 
      a[j] = a[j-1]; 
      j -= 1; 
    }
    a[j] = v; 
    cost += (i - j); // distance moved is a measure of work
    if (cost > costLimit) {
      //fprintf (stderr, "switching to the other sorting algorithm\n"); fflush (stderr);
      u32KnuthShellSort3(a, l, r); // In the Java version, this should be the system's array sort.
      return;
    }
  } 
  // The following sanity check can eventually go away, but it seems like a
  // good idea to perform it while the code is under development.
  //  Long bad = 0;
  //  for (i = l; i < r-1; i++) {
  //    if (a[i] > a[i+1]) bad++;
  //  };
  //  assert (bad == 0);
}


//  printf ("cost was %lld (arrlen=%lld)\n", cost, length); fflush (stdout);


/******************************************************/
// This merge is safe to use in carefully designed overlapping scenarios.

void u32Merge (U32 * arrA, Long startA, Long lengthA, // input
	       U32 * arrB, Long startB, Long lengthB, // input
	       U32 * arrC, Long startC) { // output
  Long lengthC = lengthA + lengthB;
  Long limA = startA + lengthA;
  Long limB = startB + lengthB;
  Long limC = startC + lengthC;
  Long a = startA;
  Long b = startB;
  Long c = startC;
  for ( ; c < limC ; c++) {
    if      (b >= limB)         { arrC[c] = arrA[a++]; }
    else if (a >= limA)         { arrC[c] = arrB[b++]; }
    else if (arrA[a] < arrB[b]) { arrC[c] = arrA[a++]; }
    else                        { arrC[c] = arrB[b++]; }
  }
  if (a != limA || b != limB) throw std::logic_error("merging error");
}



/*******************************************************/

#ifdef TOKUDA

Long tokudaIncrements [48] = 
  { 1LL, 4LL, 9LL, 20LL, 46LL, 103LL, 233LL, 525LL, 1182LL, 2660LL, 5985LL, 13467LL, 30301LL, 68178LL,
    153401LL, 345152LL, 776591LL, 1747331LL, 3931496LL, 8845866LL, 19903198LL, 44782196LL,
    100759940LL, 226709866LL, 510097200LL, 1147718700LL, 2582367076LL, 5810325920LL,
    13073233321LL, 29414774973LL, 66183243690LL, 148912298303LL, 335052671183LL,
    753868510162LL, 1696204147864LL, 3816459332694LL, 8587033498562LL,
    19320825371765LL, 43471857086472LL, 97811678444563LL, 220076276500268LL,
    495171622125603LL, 1114136149782608LL, 2506806337010868LL, 5640314258274455LL,
    12690707081117524LL, 28554090932514432LL };

// Call with r pointing AT the rightmost element of the subarray.

void u32TokudaShellSort(U32 a[], Long l, Long r)
{ Long i, h, k;
  for (k = 0; tokudaIncrements[k] <= (r-l)/5; k++) ;
  for (; k >= 0; k--) {
    h = tokudaIncrements[k];
    for (i = l+h; i <= r; i++) {
      Long j = i; U32 v = a[i]; 
      while (j >= l+h && v < a[j-h])
	{ a[j] = a[j-h]; j -= h; }
      a[j] = v; 
    } 
  }
  //  Long bad = 0;
  //  for (i = l; i < r-1; i++) {
  //    if (a[i] > a[i+1]) bad++;
  //  };
  //  assert (bad == 0);
}

#endif
