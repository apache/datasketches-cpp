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

#include "fm85Compression.h"
#include "fm85Util.h"

#include <stdexcept>
#include <new>

/*********************************/
// The following material is in a separate file because it is so big.

#include "compressionData.data"

/***************************************************************/
/***************************************************************/
// Intentionally uses malloc instead of the custom allocator
// since it is for global initialization, not for allocating instances.
U8 * makeInversePermutation (U8 * permu, int length) {
  U8 * inverse = (U8 *) malloc (((size_t) length) * sizeof(U8));
  if (inverse == NULL) throw std::bad_alloc();
  int i;
  for (i = 0; i < length; i++) {
    inverse[permu[i]] = i;
  }
  for (i = 0; i < length; i++) {
    if (permu[inverse[i]] != i) throw std::logic_error("inverse permutation error");
  }
  return inverse;
}

/***************************************************************/
/***************************************************************/

/* Given an encoding table that maps unsigned bytes to codewords 
   of length at most 12, this builds a size-4096 decoding table */

// The second argument is typically 256, but can be other values such as 65.
// Intentionally uses malloc instead of the custom allocator
// since it is for global initialization, not for allocating instances.
U16 * makeDecodingTable (U16 * encodingTable, int numByteValues) { 
  int byteValue;
  U16 * decodingTable = (U16 *) malloc (((size_t) 4096) * sizeof(U16));
  if (decodingTable == NULL) throw std::bad_alloc();
  for (byteValue=0; byteValue < numByteValues; byteValue++) {
    int encodingEntry = encodingTable [byteValue];
    int codeValue = encodingEntry & 0xfff;
    int codeLength = encodingEntry >> 12;    
    int decodingEntry = (codeLength << 8) | byteValue;
    int garbageLength = 12 - codeLength;
    int numCopies = 1 << garbageLength;
    int garbageBits;
    for (garbageBits = 0; garbageBits < numCopies; garbageBits++) {
      int extendedCodeValue = codeValue | (garbageBits << codeLength);
      decodingTable[extendedCodeValue & 0xfff] = decodingEntry;
    }
  }
  return (decodingTable);
}

/***************************************************************/
/***************************************************************/

void validateDecodingTable (U16 * decodingTable, U16 * encodingTable) {
  int decodeThis;
  
  for (decodeThis = 0; decodeThis < 4096; decodeThis++) {

    int tmpD = decodingTable[decodeThis];
    int decodedByte   = tmpD & 0xff;
    int decodedLength = tmpD >> 8;

    int tmpE = encodingTable[decodedByte];
    int encodedBitpattern = tmpE & 0xfff;
    int encodedLength = tmpE >> 12;

    // encodedBitpattern++; // uncomment this line to test the test
    // encodedLength++;     // uncomment this line to test the test

    if (decodedLength != encodedLength) throw std::logic_error("decoded length error");
    if (encodedBitpattern != (decodeThis & ((1 << decodedLength) - 1))) throw std::logic_error("bit pattern error");
  }
  
}

/***************************************************************/
/***************************************************************/

void makeTheDecodingTables (void) {
  int i;
  lengthLimitedUnaryDecodingTable65 = makeDecodingTable (lengthLimitedUnaryEncodingTable65, 65);
  validateDecodingTable (lengthLimitedUnaryDecodingTable65, lengthLimitedUnaryEncodingTable65);

  for (i = 0; i < (16 + 6); i++) {
    decodingTablesForHighEntropyByte[i] = makeDecodingTable(encodingTablesForHighEntropyByte[i], 256);
    validateDecodingTable (decodingTablesForHighEntropyByte[i], encodingTablesForHighEntropyByte[i]);
  }

  for (i = 0; i < 16; i++) {
    columnPermutationsForDecoding[i] = makeInversePermutation(columnPermutationsForEncoding[i],56);
  }

  //  fprintf (stderr, "tables okay\n"); fflush (stderr);
}

void freeTheDecodingTables (void) {
  int i;
  free(lengthLimitedUnaryDecodingTable65);
  for (i = 0; i < (16 + 6); i++) {
    free(decodingTablesForHighEntropyByte[i]);
  }
  for (i = 0; i < 16; i++) {
    free(columnPermutationsForDecoding[i]);
  }
}

/***************************************************************/
/***************************************************************/

static inline void writeUnary (U32 * compressedWords, 
			       Long * nextWordIndexPtr,
			       U64 * bitbufPtr, 
			       int * bufbitsPtr,
			       Long the_value)
{
  //  printf("%ld writing\n", the_value);

  if (compressedWords == NULL) throw std::logic_error("compressedWords == NULL");
  if (nextWordIndexPtr == NULL) throw std::logic_error("nextWordIndexPtr == NULL");
  if (bitbufPtr == NULL) throw std::logic_error("bitbufPtr == NULL");
  if (bufbitsPtr == NULL) throw std::logic_error("bufbitsPtr == NULL");

  Long nextWordIndex = *nextWordIndexPtr;
  U64  bitbuf  = *bitbufPtr;
  int  bufbits = *bufbitsPtr;
  
  if (bufbits < 0 || bufbits > 31) throw std::out_of_range("bufbits out of range");

  Long remaining = the_value;

  while (remaining >= 16) {
    remaining -= 16;
    // Here we output 16 zeros, but we don't need to physically write them into bitbuf
    // because it already contains zeros in that region.
    bufbits += 16; // Record the fact that 16 bits of output have occurred.
    MAYBE_FLUSH_BITBUF(compressedWords,nextWordIndex);
  }

  if (remaining < 0 || remaining > 15) throw std::out_of_range("remaining out of range");

  U64 theUnaryCode = 1ULL << remaining;
  bitbuf |= theUnaryCode << bufbits;
  bufbits += (1 + remaining);
  MAYBE_FLUSH_BITBUF(compressedWords,nextWordIndex);
  
  *nextWordIndexPtr = nextWordIndex;
  *bitbufPtr  = bitbuf;
  *bufbitsPtr = bufbits;

}

/***************************************************************/
/***************************************************************/

static inline Long readUnary (U32 * compressedWords, 
			      Long * nextWordIndexPtr,
			      U64 * bitbufPtr, 
			      int * bufbitsPtr)
{
  if (compressedWords == NULL) throw std::logic_error("compressedWords == NULL");
  if (nextWordIndexPtr == NULL) throw std::logic_error("nextWordIndexPtr == NULL");
  if (bitbufPtr == NULL) throw std::logic_error("bitbufPtr == NULL");
  if (bufbitsPtr == NULL) throw std::logic_error("bufbitsPtr == NULL");

  Long nextWordIndex = *nextWordIndexPtr;
  U64  bitbuf  = *bitbufPtr;
  int  bufbits = *bufbitsPtr;
  Long subTotal = 0;

 readUnaryLoop:

  MAYBE_FILL_BITBUF(compressedWords,nextWordIndex,8); // ensure 8 bits in bit buffer

  //  if (bufbits < 8) {  // Prepare for an 8-bit peek into the bitstream.
  //    bitbuf |= (((U64) compressedWords[nextWordIndex++]) << bufbits);
  //    bufbits += 32;
  //  }

  int peek8 = bitbuf & 0xffULL; // These 8 bits include either all or part of the Unary codeword.
  int trailingZeros = byteTrailingZerosTable[peek8];

  if (trailingZeros < 0 || trailingZeros > 8) throw std::out_of_range("trailingZeros out of range");

  if (trailingZeros == 8) { // The codeword was partial, so read some more.
    subTotal += 8;
    bufbits -= 8;
    bitbuf >>= 8;
    goto readUnaryLoop;
  }

  bufbits -= (1+trailingZeros);
  bitbuf >>= (1+trailingZeros);
  *nextWordIndexPtr = nextWordIndex;
  *bitbufPtr  = bitbuf;
  *bufbitsPtr = bufbits;

  //  printf("%ld READING\n", subTotal+trailingZeros); fflush (stdout);

  return (subTotal+trailingZeros);

}

/***************************************************************/
/***************************************************************/
// This returns the number of compressedWords that were actually used.
// It is the caller's responsibility to ensure that the compressedWords array is long enough.

Long lowLevelCompressBytes (U8 * byteArray,          // input
			    Long numBytesToEncode,   // input
			    U16 * encodingTable,     // input 
			    U32 * compressedWords) { // output

  Long byteIndex = 0;
  Long nextWordIndex = 0;
  
  U64 bitbuf = 0; /* bits are packed into this first, then are flushed to compressedWords */
  int bufbits = 0; /* number of bits currently in bitbuf; must be between 0 and 31 */

  for (byteIndex = 0; byteIndex < numBytesToEncode; byteIndex++) {
    U64 codeInfo = (U64) encodingTable[byteArray[byteIndex]];
    U64 codeVal = codeInfo & 0xfff;
    int codeLen = codeInfo >> 12;    
    bitbuf |= (codeVal << bufbits);
    bufbits += codeLen;
    MAYBE_FLUSH_BITBUF(compressedWords,nextWordIndex);
  }

// Pad the bitstream with 11 zero-bits so that the decompressor's 12-bit peek can't overrun its input.
  bufbits += 11; 
  MAYBE_FLUSH_BITBUF(compressedWords,nextWordIndex);

  if (bufbits > 0) { // We are done encoding now, so we flush the bit buffer.
    if (bufbits >= 32) throw std::logic_error("bufbits >= 32");
    compressedWords[nextWordIndex++] = (U32) (bitbuf & 0xffffffff); 
    bitbuf = 0; bufbits = 0; // not really necessary
  }
  return nextWordIndex;
}

/***************************************************************/
/***************************************************************/

void lowLevelUncompressBytes (U8 * byteArray,          // output
			      Long numBytesToDecode,   // input (but refers to the output)
			      U16 * decodingTable,     // input
			      U32 * compressedWords,   // input
			      Long numCompressedWords) { // input
  Long byteIndex = 0;
  Long wordIndex = 0;  

  U64 bitbuf = 0;
  int bufbits = 0;

  //  printf ("Y\n"); fflush (stdout);

  if (byteArray == NULL) throw std::logic_error("byteArray == NULL");
  if (decodingTable == NULL) throw std::logic_error("decodingTable == NULL");
  if (compressedWords == NULL) throw std::logic_error("compressedWords == NULL");

  for (byteIndex = 0; byteIndex < numBytesToDecode; byteIndex++) {

    //    if (bufbits < 12) { // Prepare for a 12-bit peek into the bitstream.
    //      bitbuf |= (((U64) compressedWords[wordIndex++]) << bufbits);
    //      bufbits += 32;
    //    }

    MAYBE_FILL_BITBUF(compressedWords,wordIndex,12); // ensure 12 bits in bit buffer

    int peek12 = bitbuf & 0xfffULL; // These 12 bits will include an entire Huffman codeword.
    int lookup = decodingTable[peek12];
    int codeWordLength = lookup >> 8;
    U8 decodedByte = lookup & 0xff;
    byteArray[byteIndex] = decodedByte;
    bitbuf >>= codeWordLength;
    bufbits -= codeWordLength;
  }
  // Buffer over-run should be impossible unless there is a bug.
  // However, we might as well check here.
  if (wordIndex > numCompressedWords) throw std::logic_error("wordIndex > numCompressedWords");

  //  printf ("X\n"); fflush (stdout);

  return;
}

/***************************************************************/
/***************************************************************/

// Here "pairs" refers to row/column pairs that specify 
// the positions of surprising values in the bit matrix.

// returns the number of compressedWords actually used
Long lowLevelCompressPairs (U32 * pairArray,       // input
			    Long numPairsToEncode, // input
			    Long numBaseBits,      // input
			    U32 * compressedWords) { // output
  Long pairIndex = 0;
  Long nextWordIndex = 0;
  U64 bitbuf = 0; 
  int bufbits = 0; 

  Long golombLoMask = (1LL << numBaseBits) - 1;

  Long  predictedRowIndex = 0;
  Short predictedColIndex = 0;

  for (pairIndex = 0; pairIndex < numPairsToEncode; pairIndex++) {
    U32 rowCol = pairArray[pairIndex];
    Long  rowIndex = (Long)  (rowCol >> 6);
    Short colIndex = (Short) (rowCol & 63);
    
    if (rowIndex != predictedRowIndex) { predictedColIndex = 0; }

    if (rowIndex < predictedRowIndex) throw std::logic_error("rowIndex < predictedRowIndex");
    if (colIndex < predictedColIndex) throw std::logic_error("colIndex < predictedColIndex");

    Long  yDelta = rowIndex - predictedRowIndex;
    Short xDelta = colIndex - predictedColIndex;

    predictedRowIndex = rowIndex;
    predictedColIndex = colIndex + 1;

    U64 codeInfo = (U64) lengthLimitedUnaryEncodingTable65[xDelta];
    U64 codeVal = codeInfo & 0xfff;
    int codeLen = codeInfo >> 12;    
    bitbuf |= (codeVal << bufbits);
    bufbits += codeLen;
    MAYBE_FLUSH_BITBUF(compressedWords,nextWordIndex);

    Long golombLo = yDelta & golombLoMask;
    Long golombHi = yDelta >> numBaseBits;

    writeUnary (compressedWords, &nextWordIndex, &bitbuf, &bufbits, golombHi);

    bitbuf |= golombLo << bufbits;
    bufbits += numBaseBits;
    MAYBE_FLUSH_BITBUF(compressedWords,nextWordIndex);
  }  

  // Pad the bitstream so that the decompressor's 12-bit peek can't overrun its input.
  Long padding = 10LL - numBaseBits; // should be 10LL
  if (padding < 0) padding = 0;
  bufbits += padding; 
  MAYBE_FLUSH_BITBUF(compressedWords,nextWordIndex);

  if (bufbits > 0) { // We are done encoding now, so we flush the bit buffer.
    if (bufbits >= 32) throw std::logic_error("bufbits >= 32");
    compressedWords[nextWordIndex++] = (U32) (bitbuf & 0xffffffff); 
    bitbuf = 0; bufbits = 0; // not really necessary
  }
  return nextWordIndex;

}

/***************************************************************/
/***************************************************************/

void lowLevelUncompressPairs (U32 * pairArray,         // output
			      Long numPairsToDecode,   // input (but refers to the output)
			      Long numBaseBits,        // input
			      U32 * compressedWords,   // input
			      Long numCompressedWords) { // input
  Long pairIndex = 0;
  Long wordIndex = 0;
  U64 bitbuf = 0; 
  int bufbits = 0; 

  Long golombLoMask = (1LL << numBaseBits) - 1;

  Long  predictedRowIndex = 0;
  Short predictedColIndex = 0;

  // for each pair we need to read:
  // xDelta (12-bit length-limited unary)
  // yDeltaHi (unary)
  // yDeltaLo (basebits)

  for (pairIndex = 0; pairIndex < numPairsToDecode; pairIndex++) {

    MAYBE_FILL_BITBUF(compressedWords,wordIndex,12); // ensure 12 bits in bit buffer
    int peek12 = bitbuf & 0xfffULL;
    int lookup = lengthLimitedUnaryDecodingTable65[peek12];
    int codeWordLength = lookup >> 8;
    Short xDelta = lookup & 0xff;
    bitbuf >>= codeWordLength;
    bufbits -= codeWordLength;

    Long golombHi = readUnary (compressedWords, &wordIndex, &bitbuf, &bufbits);

    MAYBE_FILL_BITBUF(compressedWords,wordIndex,numBaseBits); // ensure numBaseBits in bit buffer
    Long golombLo = bitbuf & golombLoMask;
    bitbuf >>= numBaseBits;
    bufbits -= numBaseBits;
    Long yDelta = (golombHi << numBaseBits) | golombLo;

    // Now that we have yDelta and xDelta, we can compute the pair's row and column.
    if (yDelta > 0) { predictedColIndex = 0; }
    Long  rowIndex = predictedRowIndex + yDelta;
    Short colIndex = predictedColIndex + xDelta;
    U32 rowCol = (rowIndex << 6) | colIndex;
    pairArray[pairIndex] = rowCol;
    predictedRowIndex = rowIndex;
    predictedColIndex = colIndex + 1;

  }
  if (wordIndex > numCompressedWords) throw std::logic_error("wordIndex > numCompressedWords"); // check for buffer over-run
}


/***************************************************************/
/***************************************************************/

Long safeLengthForCompressedPairBuf (Long k, Long numPairs, Long numBaseBits) {
  if (numPairs <= 0) throw std::invalid_argument("numPairs <= 0");
  // Long ybits = k + numPairs; // simpler and safer UB
  // The following tighter UB on ybits is based on page 198 
  // of the textbook "Managing Gigabytes" by Witten, Moffat, and Bell.
  // Notice that if numBaseBits == 0 it coincides with (k + numPairs).
  Long ybits = numPairs * (1LL + numBaseBits) + (k >> numBaseBits);
  Long xbits = 12 * numPairs;
  Long padding = 10LL - numBaseBits;
  if (padding < 0) padding = 0;
  Long bits = xbits + ybits + padding;
  return (divideLongsRoundingUp(bits, 32));
}

// Explanation of padding: we write 
// 1) xdelta (huffman, provides at least 1 bit, requires 12-bit lookahead)
// 2) ydeltaGolombHi (unary, provides at least 1 bit, requires 8-bit lookahead)
// 3) ydeltaGolombLo (straight B bits).
// So the 12-bit lookahead is the tight constraint, but there are at least (2 + B) bits emitted,
// so we would be safe with max (0, 10 - B) bits of padding at the end of the bitstream.

/***************************************************************/

Long safeLengthForCompressedWindowBuf (Long k) { // measured in 32-bit words
  Long bits = 12 * k + 11; // 11 bits of padding, due to 12-bit lookahead, with 1 bit certainly present.
  return (divideLongsRoundingUp(bits, 32));
}

/***************************************************************/
/***************************************************************/

Short determinePseudoPhase (Short lgK, Long c) {
  Long k = (1LL << lgK);  
 // This midrange logic produces pseudo-phases. They are used to select encoding tables.
 // The thresholds were chosen by hand after looking at plots of measured compression.
  if (1000 * c < 2375 * k) {
    if      (   4 * c <    3 * k) return ( 16 + 0 );  // midrange table
    else if (  10 * c <   11 * k) return ( 16 + 1 );  // midrange table
    else if ( 100 * c <  132 * k) return ( 16 + 2 );  // midrange table
    else if (   3 * c <    5 * k) return ( 16 + 3 );  // midrange table
    else if (1000 * c < 1965 * k) return ( 16 + 4 );  // midrange table
    else if (1000 * c < 2275 * k) return ( 16 + 5 );  // midrange table
    else return ( 6 );  // steady-state table employed before its actual phase
  } 
  else { // This steady-state logic produces true phases. They are used to select
         // encoding tables, and also column permutations for the "Sliding" flavor.
    if (lgK < 4) throw std::logic_error("lgK < 4");
    Long tmp = c >> (lgK - 4);
    Long phase = tmp & 15;
    if (phase < 0 || phase >= 16) throw std::out_of_range("wrong phase");
    return ((Short) phase);
  }
}

/***************************************************************/
/***************************************************************/

void compressTheWindow (FM85 * target, FM85 * source) {
  Long k = (1LL << source->lgK);  
  Long windowBufLen = safeLengthForCompressedWindowBuf (k);
  U32 * windowBuf = (U32 *) fm85alloc ((size_t) (windowBufLen * sizeof(U32)));
  if (windowBuf == NULL) throw std::logic_error("windowBuf == NULL");
  Short pseudoPhase = determinePseudoPhase (source->lgK, source->numCoupons);
  target->cwLength = lowLevelCompressBytes (source->slidingWindow, k,
					    encodingTablesForHighEntropyByte[pseudoPhase],
					    windowBuf);

  // At this point we free the unused portion of the compression output buffer.
  // Note: realloc caused strange timing spikes for lgK = 11 and 12.

  U32 * shorterBuf = (U32 *) fm85alloc (((size_t) target->cwLength) * sizeof(U32));
  if (shorterBuf == NULL) throw std::bad_alloc();
  memcpy ((void *) shorterBuf, (void *) windowBuf, ((size_t) target->cwLength) * sizeof(U32));
  fm85free (windowBuf);
  target->compressedWindow = shorterBuf;

  return;
}

/***************************************************************/
/***************************************************************/

void uncompressTheWindow (FM85 * target, FM85 * source) {
  Long k = (1LL << source->lgK);  
  U8 * window = (U8 *) fm85alloc ((size_t) (k * sizeof(U8)));
  if (window == NULL) throw std::bad_alloc();
  // zeroing not needed here (unlike the Hybrid Flavor)
  if (target->slidingWindow != NULL) throw std::logic_error("target->slidingWindow != NULL");
  target->slidingWindow = window;
  Short pseudoPhase = determinePseudoPhase (source->lgK, source->numCoupons);
  if (source->compressedWindow == NULL) throw std::logic_error("source->compressedWindow == NULL");
  lowLevelUncompressBytes (target->slidingWindow, k,
			   decodingTablesForHighEntropyByte[pseudoPhase],
			   source->compressedWindow,
			   source->cwLength);
  return;
}

/***************************************************************/
/***************************************************************/

void compressTheSurprisingValues (FM85 * target, FM85 * source, U32 * pairs, Long numPairs) {
  if (numPairs <= 0) throw std::invalid_argument("numPairs <= 0");
  target->numCompressedSurprisingValues = numPairs;  
  Long k = (1LL << source->lgK);
  Long numBaseBits = golombChooseNumberOfBaseBits (k + numPairs, numPairs);
  Long pairBufLen = safeLengthForCompressedPairBuf (k, numPairs, numBaseBits);
  U32 * pairBuf = (U32 *) fm85alloc ((size_t) (pairBufLen * sizeof(U32)));
  if (pairBuf == NULL) throw std::bad_alloc();

  target->csvLength = lowLevelCompressPairs (pairs, numPairs, numBaseBits, pairBuf);

  // At this point we free the unused portion of the compression output buffer.
  // Note: realloc caused strange timing spikes for lgK = 11 and 12.

  U32 * shorterBuf = (U32 *) fm85alloc (((size_t) target->csvLength) * sizeof(U32));
  if (shorterBuf == NULL) throw std::bad_alloc();
  memcpy ((void *) shorterBuf, (void *) pairBuf, ((size_t) target->csvLength) * sizeof(U32));
  fm85free (pairBuf);
  target->compressedSurprisingValues = shorterBuf;
}

/***************************************************************/
/***************************************************************/
// allocates and returns an array of uncompressed pairs.
// the length of this array is known to the source sketch.

U32 * uncompressTheSurprisingValues (FM85 * source) {
  if (source->isCompressed != 1) throw std::logic_error("not compressed");
  Long k = (1LL << source->lgK);  
  Long numPairs = source->numCompressedSurprisingValues;
  if (numPairs <= 0) throw std::logic_error("numPairs <= 0");
  U32 * pairs = (U32 *) fm85alloc ((size_t) numPairs * sizeof(U32));
  if (pairs == NULL) throw std::bad_alloc();
  Long numBaseBits = golombChooseNumberOfBaseBits (k + numPairs, numPairs);
  lowLevelUncompressPairs(pairs, numPairs, numBaseBits, 
			  source->compressedSurprisingValues, source->csvLength);
  return (pairs);
}

/***************************************************************/
/***************************************************************/

void compressEmptyFlavor (FM85 * target, FM85 * source) {
  return; // nothing to do, so just return
}

/***************************************************************/

void uncompressEmptyFlavor (FM85 * target, FM85 * source) {
  return; // nothing to do, so just return
}

/***************************************************************/
/***************************************************************/

void compressSparseFlavor (FM85 * target, FM85 * source) {
  if (source->slidingWindow != NULL) throw std::logic_error("source->slidingWindow != NULL"); // there is no window to compress
  Long numPairs = 0;
  U32 * pairs = u32TableUnwrappingGetItems (source->surprisingValueTable, &numPairs);
  introspectiveInsertionSort(pairs, 0, numPairs-1);
  compressTheSurprisingValues (target, source, pairs, numPairs);
  if (pairs) fm85free (pairs);
  return;
}

/***************************************************************/

void uncompressSparseFlavor (FM85 * target, FM85 * source) {
  if (source->compressedWindow != NULL) throw std::logic_error("source->compressedWindow != NULL");
  if (source->compressedSurprisingValues == NULL) throw std::logic_error("source->compressedSurprisingValues == NULL");
  U32 * pairs = uncompressTheSurprisingValues (source);
  Long numPairs = source->numCompressedSurprisingValues;
  u32Table * table = makeU32TableFromPairsArray (pairs, numPairs, source->lgK);
  target->surprisingValueTable = table;
  fm85free (pairs);
  return;
}

/***************************************************************/
/***************************************************************/
// The empty space that this leaves at the beginning of the output array
// will be filled in later by the caller.

U32 * trickyGetPairsFromWindow (U8 * window, Long k, Long numPairsToGet, Long emptySpace) {
  Long outputLength = emptySpace + numPairsToGet;
  U32 * pairs = (U32 *) fm85alloc ((size_t) (outputLength * sizeof(U32)));
  if (pairs == NULL) throw std::bad_alloc();
  Long rowIndex = 0;
  Long pairIndex = emptySpace;
  for (rowIndex = 0; rowIndex < k; rowIndex++) {
    U8 byte = window[rowIndex];
    while (byte != 0) {
      Short colIndex = byteTrailingZerosTable[byte];
      //      assert (colIndex < 8);
      byte = byte ^ (1 << colIndex); // erase the 1
      pairs[pairIndex++] = (U32) ((rowIndex << 6) | colIndex);
    }
  }
  if (pairIndex != outputLength) throw std::logic_error("pairIndex != outputLength");
  return (pairs);
}

/***************************************************************/
// This is complicated because it effectively builds a Sparse version
// of a Pinned sketch before compressing it. Hence the name Hybrid.

void compressHybridFlavor (FM85 * target, FM85 * source) {
  //  Long i;
  Long k = (1LL << source->lgK);
  Long numPairsFromTable = 0; 
  U32 * pairsFromTable = u32TableUnwrappingGetItems (source->surprisingValueTable, &numPairsFromTable);
  introspectiveInsertionSort(pairsFromTable, 0, numPairsFromTable-1);
  if (source->slidingWindow == NULL) throw std::logic_error("source->slidingWindow == NULL");
  if (source->windowOffset != 0) throw std::logic_error("source->windowOffset != 0");
  Long numPairsFromArray = source->numCoupons - numPairsFromTable; // because the window offset is zero

  U32 * allPairs = trickyGetPairsFromWindow (source->slidingWindow, k, numPairsFromArray, numPairsFromTable);

  u32Merge (pairsFromTable, 0, numPairsFromTable,
	    allPairs, numPairsFromTable, numPairsFromArray,
	    allPairs, 0);  // note the overlapping subarray trick

  //  for (i = 0; i < source->numCoupons-1; i++) { assert (allPairs[i] < allPairs[i+1]); }

  compressTheSurprisingValues (target, source, allPairs, source->numCoupons);
  if (pairsFromTable) fm85free (pairsFromTable);
  fm85free (allPairs);
  return;
}

/***************************************************************/

void uncompressHybridFlavor (FM85 * target, FM85 * source) {
  if (source->compressedWindow != NULL) throw std::logic_error("source->compressedWindow != NULL");
  if (source->compressedSurprisingValues == NULL) throw std::logic_error("source->compressedSurprisingValues == NULL");
  U32 * pairs = uncompressTheSurprisingValues (source);
  Long numPairs = source->numCompressedSurprisingValues;
  // In the hybrid flavor, some of these pairs actually
  // belong in the window, so we will separate them out,
  // moving the "true" pairs to the bottom of the array.

  Long k = (1LL << source->lgK);

  U8 * window = (U8 *) fm85alloc ((size_t) (k * sizeof(U8)));
  if (window == NULL) throw std::bad_alloc();
  bzero ((void *) window, (size_t) k); // important: zero the memory
  
  Long nextTruePair = 0;
  Long i;

  for (i = 0; i < numPairs; i++) {
    U32 rowCol = pairs[i];
    if (rowCol == ALL32BITS) throw std::logic_error("rowCol == ALL32BITS");
    Short col = (Short) (rowCol & 63);
    if (col < 8) {
      Long  row = (Long) (rowCol >> 6);
      window[row] |= (1 << col); // set the window bit
    }
    else {
      pairs[nextTruePair++] = rowCol; // move true pair down
    }
  }

  if (source->windowOffset != 0) throw std::logic_error("source->windowOffset != 0");
  target->windowOffset = 0;

  u32Table * table = makeU32TableFromPairsArray (pairs, 
						 nextTruePair,
						 source->lgK);
  target->surprisingValueTable = table;
  target->slidingWindow = window;

  fm85free (pairs);

  return;
}

/***************************************************************/
/***************************************************************/

void compressPinnedFlavor (FM85 * target, FM85 * source) {

  compressTheWindow (target, source);

  Long numPairs = source->surprisingValueTable->numItems;
  if (numPairs > 0) {
    Long chkNumPairs;
    U32 * pairs = u32TableUnwrappingGetItems (source->surprisingValueTable, &chkNumPairs);
    if (chkNumPairs != numPairs) throw std::logic_error("chkNumPairs != numPairs");

    // Here we subtract 8 from the column indices.  Because they are stored in the low 6 bits 
    // of each rowCol pair, and because no column index is less than 8 for a "Pinned" sketch,
    // I believe we can simply subtract 8 from the pairs themselves.

    Long i; // shift the columns over by 8 positions before compressing (because of the window)
    for (i = 0; i < numPairs; i++) { 
      if ((pairs[i] & 63) < 8) throw std::logic_error("(pairs[i] & 63) < 8");
      pairs[i] -= 8; 
    }

    introspectiveInsertionSort(pairs, 0, numPairs-1);
    compressTheSurprisingValues (target, source, pairs, numPairs);
    if (pairs) fm85free (pairs);
  }
  return;
}

/***************************************************************/

void uncompressPinnedFlavor (FM85 * target, FM85 * source) {
  if (source->compressedWindow == NULL) throw std::logic_error("source->compressedWindow == NULL");
  uncompressTheWindow (target, source);
  Long numPairs = source->numCompressedSurprisingValues;
  if (numPairs == 0) {
    target->surprisingValueTable = u32TableMake (2, 6 + source->lgK);
  }
  else {
    if (numPairs <= 0) throw std::logic_error("numPairs <= 0");
    if (source->compressedSurprisingValues == NULL) throw std::logic_error("source->compressedSurprisingValues == NULL");
    U32 * pairs = uncompressTheSurprisingValues (source);
    Long i; // undo the compressor's 8-column shift
    for (i = 0; i < numPairs; i++) { 
      if ((pairs[i] & 63) >= 56) throw std::logic_error("(pairs[i] & 63) >= 56");
      pairs[i] += 8; 
    }
    u32Table * table = makeU32TableFromPairsArray (pairs, numPairs, source->lgK);
    target->surprisingValueTable = table;
    fm85free (pairs);
  }
  return;
}

/***************************************************************/
/***************************************************************/
// Complicated by the existence of both a left fringe and a right fringe.

void compressSlidingFlavor (FM85 * target, FM85 * source) {

  compressTheWindow (target, source);

  Long numPairs = source->surprisingValueTable->numItems;

  if (numPairs > 0) {
    Long chkNumPairs;
    U32 * pairs = u32TableUnwrappingGetItems (source->surprisingValueTable, &chkNumPairs);
    if (chkNumPairs != numPairs) throw std::logic_error("chkNumPairs != numPairs");

    // Here we apply a complicated transformation to the column indices, which
    // changes the implied ordering of the pairs, so we must do it before sorting.

    Short pseudoPhase = determinePseudoPhase (source->lgK, source->numCoupons); // NB
    if (pseudoPhase >= 16) throw std::logic_error("pseudoPhase >= 16");
    U8 * permutation = columnPermutationsForEncoding[pseudoPhase];

    Short offset = source->windowOffset;
    if (offset <= 0 || offset > 56) throw std::out_of_range("offset out of range");

    Long i; 
    for (i = 0; i < numPairs; i++) { 
      U32 rowCol = pairs[i];
      Long  row = (Long)  (rowCol >> 6);
      Short col = (Short) (rowCol & 63);
      // first rotate the columns into a canonical configuration: new = ((old - (offset+8)) + 64) mod 64
      col = (col + 56 - offset) & 63;
      if (col < 0 || col >= 56) throw std::out_of_range("col out of range");
      // then apply the permutation
      col = permutation[col];
      pairs[i] = (U32) ((row << 6) | col);
    }

    introspectiveInsertionSort(pairs, 0, numPairs-1);
    compressTheSurprisingValues (target, source, pairs, numPairs);
    if (pairs) fm85free (pairs);
  }
  return;
}

/***************************************************************/

void uncompressSlidingFlavor (FM85 * target, FM85 * source) {
  if (source->compressedWindow == NULL) throw std::logic_error("source->compressedWindow == NULL");
  uncompressTheWindow (target, source);

  Long numPairs = source->numCompressedSurprisingValues;
  if (numPairs == 0) {
    target->surprisingValueTable = u32TableMake (2, 6 + source->lgK);
  }
  else {
    if (numPairs <= 0) throw std::logic_error("numPairs <= 0");
    if (source->compressedSurprisingValues == NULL) throw std::logic_error("source->compressedSurprisingValues == NULL");
    U32 * pairs = uncompressTheSurprisingValues (source);

    Short pseudoPhase = determinePseudoPhase (source->lgK, source->numCoupons); // NB
    if (pseudoPhase >= 16) throw std::logic_error("pseudoPhase >= 16");
    U8 * permutation = columnPermutationsForDecoding[pseudoPhase];

    Short offset = source->windowOffset;
    if (offset <= 0 || offset > 56) throw std::out_of_range("offset out of range");

    Long i; 
    for (i = 0; i < numPairs; i++) { 
      U32 rowCol = pairs[i];
      Long  row = (Long)  (rowCol >> 6);
      Short col = (Short) (rowCol & 63);
      // first undo the permutation
      col = permutation[col];
      // then undo the rotation: old = (new + (offset+8)) mod 64
      col = (col + (offset+8)) & 63;
      pairs[i] = (U32) ((row << 6) | col);
    }

    u32Table * table = makeU32TableFromPairsArray (pairs, numPairs, source->lgK);
    target->surprisingValueTable = table;

    fm85free (pairs);
  }
  return;
}

/***************************************************************/
/***************************************************************/

// Note: in the final system, compressed and uncompressed sketches will have different types

FM85 * fm85Compress (FM85 * source) {
  if (source->isCompressed != 0) throw std::invalid_argument("already compressed");

  FM85 * target = (FM85 *) fm85alloc (sizeof(FM85));
  if (target == NULL) throw std::bad_alloc();

  target->lgK = source->lgK;
  target->numCoupons = source->numCoupons;
  target->windowOffset = source->windowOffset;
  target->firstInterestingColumn = source->firstInterestingColumn;
  target->mergeFlag = source->mergeFlag;
  target->kxp = source->kxp;
  target->hipEstAccum = source->hipEstAccum;
  target->hipErrAccum = source->hipErrAccum;

  target->isCompressed = 1;

  // initialize the variables that belong in a compressed sketch
  target->numCompressedSurprisingValues = 0;
  target->compressedSurprisingValues = (U32 *) NULL;
  target->csvLength = 0;
  target->compressedWindow = (U32 *) NULL;
  target->cwLength = 0;

  // clear the variables that don't belong in a compressed sketch
  target->slidingWindow = NULL;
  target->surprisingValueTable = NULL;

  enum flavorType flavor = determineSketchFlavor(source);
  switch (flavor) {
  case EMPTY: compressEmptyFlavor  (target, source); break;
  case SPARSE:
    compressSparseFlavor (target, source); 
    if (target->compressedWindow != NULL) throw std::logic_error("target->compressedWindow != NULL");
    if (target->compressedSurprisingValues == NULL) throw std::logic_error("target->compressedSurprisingValues == NULL");
    break;
  case HYBRID:  
    compressHybridFlavor (target, source); 
    if (target->compressedWindow != NULL) throw std::logic_error("target->compressedWindow != NULL");
    if (target->compressedSurprisingValues == NULL) throw std::logic_error("target->compressedSurprisingValues == NULL");
    break;
  case PINNED:  
    compressPinnedFlavor (target, source); 
    if (target->compressedWindow == NULL) throw std::logic_error("target->compressedWindow != NULL");
    //    assert (target->compressedSurprisingValues != NULL);
    break;
  case SLIDING: 
    compressSlidingFlavor(target, source); 
    if (target->compressedWindow == NULL) throw std::logic_error("target->compressedWindow == NULL");
    //    assert (target->compressedSurprisingValues != NULL);
    break;
  default: throw std::logic_error("Unknown sketch flavor");
  }

  return target;
}

/***************************************************************/
/***************************************************************/
// Note: in the final system, compressed and uncompressed sketches will have different types

FM85 * fm85Uncompress (FM85 * source) {
  if (source->isCompressed != 1) throw std::invalid_argument("not compressed");

  FM85 * target = (FM85 *) fm85alloc (sizeof(FM85));
  if (target == NULL) throw std::bad_alloc();

  target->lgK = source->lgK;
  target->numCoupons = source->numCoupons;
  target->windowOffset = source->windowOffset;
  target->firstInterestingColumn = source->firstInterestingColumn;
  target->mergeFlag = source->mergeFlag;
  target->kxp = source->kxp;
  target->hipEstAccum = source->hipEstAccum;
  target->hipErrAccum = source->hipErrAccum;

  target->isCompressed = 0;

  // initialize the variables that belong in an updateable sketch
  target->slidingWindow = (U8 *) NULL;
  target->surprisingValueTable = (u32Table *) NULL;

  // clear the variables that don't belong in an updateable sketch
  target->numCompressedSurprisingValues = 0;
  target->compressedSurprisingValues = (U32 *) NULL;
  target->csvLength = 0;
  target->compressedWindow = (U32 *) NULL;
  target->cwLength = 0;

  enum flavorType flavor = determineSketchFlavor(source);
  switch (flavor) {
  case EMPTY: uncompressEmptyFlavor  (target, source); break;
  case SPARSE:  
    if (source->compressedWindow != NULL) throw std::logic_error("source->compressedWindow != NULL");
    uncompressSparseFlavor (target, source); 
    break;
  case HYBRID:  
    uncompressHybridFlavor (target, source); 
    break;
  case PINNED:
    if (source->compressedWindow == NULL) throw std::logic_error("source->compressedWindow == NULL");
    uncompressPinnedFlavor (target, source);
    break;
  case SLIDING: uncompressSlidingFlavor(target, source); break;
  default: std::logic_error("Unknown sketch flavor");
  }

  return target;
}
