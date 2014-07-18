/*
$LicenseInfo:firstyear=2010&license=mit$

Copyright (c) 2010, Linden Research, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
$/LicenseInfo$

Source : http://hg.secondlife.com/llsd/src/tip/js/typedarray.js

*/
/*global document*/


var Float64Array;

sx.ieee754 = (function () {
    "use strict";

    var unpackFloat64;

    var unpackIEEE754 = function unpackIEEE754(bytes, ebits, fbits) {

        // Bytes to bits
        var bits = [], i, j, b, str,
            bias, s, e, f;

        for (i = bytes.length; i; i -= 1) {
            b = bytes[i - 1];
            for (j = 8; j; j -= 1) {
                bits.push(b % 2 ? 1 : 0); b = b >> 1;
            }
        }
        bits.reverse();
        str = bits.join('');

        // Unpack sign, exponent, fraction
        bias = (1 << (ebits - 1)) - 1;
        s = parseInt(str.substring(0, 1), 2) ? -1 : 1;
        e = parseInt(str.substring(1, 1 + ebits), 2);
        f = parseInt(str.substring(1 + ebits), 2);

        // Produce number
        if (e === (1 << ebits) - 1) {
            return f !== 0 ? NaN : s * Infinity;
        }
        else if (e > 0) {
            // Normalized
            return s * Math.pow(2, e - bias) * (1 + f / Math.pow(2, fbits));
        }
        else if (f !== 0) {
            // Denormalized
            return s * Math.pow(2, -(bias - 1)) * (f / Math.pow(2, fbits));
        }
        else {
            return s < 0 ? -0 : 0;
        }
    }

    unpackFloat64 = function unpackFloat64(b) {
        return unpackIEEE754(b, 11, 52);
    }

    return {
        unpackFloat64: unpackFloat64
    };

} ());
