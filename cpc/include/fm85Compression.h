/*
 * Copyright 2018, Oath Inc. Licensed under the terms of the
 * Apache License 2.0. See LICENSE file at the project root for terms.
 */

// author Kevin Lang, Oath Research

#ifndef GOT_FM85_COMPRESSION_H
#include "common.h"
#include "fm85.h"
#include "fm85Util.h"

/****************************************/

#define MAYBE_FLUSH_BITBUF(wordarr,wordindex) \
   if (bufbits >= 32) { \
   (wordarr)[(wordindex)++] = (U32) (bitbuf & 0xffffffff); \
    bitbuf = bitbuf >> 32; bufbits -= 32;}


#define MAYBE_FILL_BITBUF(wordarr,wordindex,minbits) \
  if (bufbits < (minbits)) { \
      bitbuf |= (((U64) (wordarr)[(wordindex)++]) << bufbits);	\
      bufbits += 32; \
    }

/****************************************/

void makeTheDecodingTables (void); // call this at startup
void freeTheDecodingTables (void); // call this at the end

/****************************************/
// Here "pairs" refers to row/column pairs that specify 
// the positions of surprising values in the bit matrix.

// returns the number of compressedWords actually used
Long lowLevelCompressPairs (U32 * pairArray, // input
			    Long numPairs, // input
			    Long numBaseBits,      // input
			    U32 * compressedWords); // output

void lowLevelUncompressPairs (U32 * pairArray, // output
			      Long numPairs, // input
			      Long numBaseBits,      // input
			      U32 * compressedWords, // input
			      Long numCompressedWords); // input

/****************************************/

// This returns the number of compressedWords that were actually used. It is the caller's 
// responsibility to ensure that the compressedWords array is long enough to prevent over-run.

Long lowLevelCompressBytes (U8 * byteArray,         // input
			    Long numBytesToEncode,  // input
			    U16 * encodingTable,    // input
			    U32 * compressedWords); // output

/****************************************/

void lowLevelUncompressBytes (U8 * byteArray,         // output
			      Long numBytesToDecode,  // input (but refers to the output)
			      U16 * decodingTable,    // input
			      U32 * compressedWords, // input
			      Long numCompressedWords); // input

/****************************************/

FM85 * fm85Compress (FM85 * uncompressedSketch); // returns a compressed copy of its input

FM85 * fm85Uncompress (FM85 * compressedSketch); // returns an updateable copy of its input

// Note: in the final system, compressed and uncompressed sketches will have different types

/****************************************/

#define GOT_FM85_COMPRESSION_H
#endif


