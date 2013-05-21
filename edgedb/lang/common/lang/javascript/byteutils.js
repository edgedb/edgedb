/*
* Copyright (c) 2013 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


sx.byteutils = (function() {
    'use strict';

    function _utf8_encode(str) {
        if (!str) {
            return '';
        }
        return unescape(encodeURIComponent(String(str)));
    };

    return {
        unhexlify: function(hex) {
            // equivalent to python's `binascii.unhexlify`

            var len = hex.length,
                i, j, bytes;

            if (len % 2 == 1) {
                throw new Error('Odd-length string');
            }

            bytes = new Array(len >> 1);

            for (i = j = 0; i < len; i += 2, j++) {
                bytes[j] = parseInt(hex[i] + hex[i+1], 16);
            }

            return bytes;
        },

        to_bytes: function(str) {
            // encodes JS unicode string to utf8, and returns
            // array of bytes

            var str = _utf8_encode(str),
                len = str.length,
                i,
                res = new Array(len);

            for (i = 0; i < len; i++) {
                res[i] = str.charCodeAt(i);
            }

            return res;
        }
    }
})();
