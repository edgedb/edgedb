/*
* Copyright (c) 2013 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from . import sx, byteutils


sx.base64 = (function() {
    'use strict';

    var pads = [[0], [0, 0], [0, 0, 0], [0, 0, 0, 0], [0, 0, 0, 0, 0]],
        _caps = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ',
        tab32 = _caps + '234567',
        tab64 = _caps + _caps.toLowerCase() + '0123456789+/=',
        overs32 = ['======', '====', '===', '='],
        overs64 = ['==', '='];

    function _cast_msg(msg) {
        if (sx.is_string(msg)) {
            return sx.byteutils.to_bytes(msg);
        } else {
            return [].concat(msg || []);
        }
    }

    function b32encode(msg) {
        // produces exactly the same results as python `base64.b32encode`

        msg = _cast_msg(msg);

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

            encoded += tab32[c1 >>> 3];
            encoded += tab32[((c1 & 0x7) << 2) + (c2 >>> 6)];
            encoded += tab32[(c2 >>> 1) & 0x1F];
            encoded += tab32[((c2 & 0x1) << 4) + (c3 >>> 4)];
            encoded += tab32[((c3 & 0xF) << 1) + (c4 >>> 7)];
            encoded += tab32[(c4 >>> 2) & 0x1F];
            encoded += tab32[((c4 & 0x3) << 3) + (c5 >>> 5)];
            encoded += tab32[c5 & 0x1F];
        }

        if (leftover) {
            i = overs32[leftover - 1];
            return encoded.substring(0, encoded.length - i.length) + i;
        }

        return encoded;
    }

    function b64encode(msg) {
        msg = _cast_msg(msg);

        var len = msg.length,
            quanta = Math.floor(len / 3),
            leftover = len % 3,
            i, k, c1, c2, c3,
            encoded = '';

        if (leftover) {
            msg.push.apply(msg, pads[3 - leftover]);
            quanta++;
        }

        for (i = 0, k = 0; i < quanta; ++i, k = i * 3) {
            c1 = msg[k];
            c2 = msg[k+1];
            c3 = msg[k+2];

            encoded += tab64[c1 >>> 2];
            encoded += tab64[((c1 & 3) << 4) | (c2 >> 4)];
            encoded += tab64[((c2 & 15) << 2) | (c3 >> 6)];
            encoded += tab64[c3 & 63];
        }

        if (leftover) {
            i = overs64[leftover - 1];
            return encoded.substring(0, encoded.length - i.length) + i;
        }

        return encoded;
    }

    function b64decode(msg) {
        var result = [],
            i = 0,
            len = msg.length,
            e1, e2, e3, e4;

        while (i < len) {
            e1 = tab64.indexOf(msg[i++]);
            e2 = tab64.indexOf(msg[i++]);
            e3 = tab64.indexOf(msg[i++]);
            e4 = tab64.indexOf(msg[i++]);

            result.push((e1 << 2) | (e2 >> 4));
            if (e3 != 64) {
                result.push(((e2 & 15) << 4) | (e3 >> 2));
                if (e4 != 64) {
                    result.push(((e3 & 3) << 6) | e4);
                }
            }
        }

        return result;
    }

    return {
        b32encode: b32encode,
        b64encode: b64encode,
        b64decode: b64decode,
        b64decode_tostr: function(m) {
            return sx.byteutils.from_bytes(b64decode(m));
        }
    };
})();
