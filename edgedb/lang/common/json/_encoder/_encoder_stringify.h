/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
*/

#ifndef ___ENCODER_STRINGIFY_H__
#define ___ENCODER_STRINGIFY_H__

#include "_encoder_buffer.h"

/* prints 'n' to buffer 'encodedData'. There should be at most 16 decimal digits */
static void longlong_to_string (long long n, EncodedData * encodedData);

/* prints 'n' to buffer 'encodedData', filling with 0s on the left until
 * printed size is 'fill_to_size' characters. 'n' sould have at most 6 significant digits */
static void datevalue_to_string (unsigned int n, EncodedData * encodedData, int fill_to_size);

#endif
