/*
* Copyright (c) 2013 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from . import sx, byteutils


sx.base64 = (function() {
    'use strict';

    var b32encode = (function() {
        // produces exactly the same results as python `base64.b32encode`

        var pads = [[0], [0, 0], [0, 0, 0], [0, 0, 0, 0]],
            tab = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L',
                   'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X',
                   'Y', 'Z', '2', '3', '4', '5', '6', '7'],
            overs = ['======', '====', '===', '='];

        return function b32encode(msg) {
            if (sx.is_string(msg)) {
                msg = sx.byteutils.to_bytes(msg);
            }

            var len = msg.length,
                quanta = Math.floor(len / 5),
                leftover = len % 5,
                i, k, c1, c2, c3, c4, c5,
                encoded = '';

            if (leftover) {
                msg.push.apply(msg, pads[5 - leftover]);
                quanta++;
            }

            for (i = 0, k = 0; i < quanta; ++i, k = i * 5) {
                c1 = msg[k];
                c2 = msg[k+1];
                c3 = msg[k+2];
                c4 = msg[k+3];
                c5 = msg[k+4];

                encoded += tab[c1 >>> 3];
                encoded += tab[((c1 & 0x7) << 2) + (c2 >>> 6)];
                encoded += tab[(c2 >>> 1) & 0x1F];
                encoded += tab[((c2 & 0x1) << 4) + (c3 >>> 4)];
                encoded += tab[((c3 & 0xF) << 1) + (c4 >>> 7)];
                encoded += tab[(c4 >>> 2) & 0x1F];
                encoded += tab[((c4 & 0x3) << 3) + (c5 >>> 5)];
                encoded += tab[c5 & 0x1F];
            }

            if (leftover) {
                i = overs[leftover - 1];
                return encoded.substring(0, encoded.length - i.length) + i;
            }

            return encoded;
        }
    })();

    return {
        b32encode: b32encode
    };
})();
