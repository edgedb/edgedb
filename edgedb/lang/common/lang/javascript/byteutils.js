/*
* Copyright (c) 2013 MagicStack Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


sx.byteutils = (function() {
    'use strict';

    var hex = '0123456789abcdef';

    function _utf8_encode(str) {
        if (!str) {
            return '';
        }
        return unescape(encodeURIComponent(String(str)));
    };

    function _utf8_decode(str) {
        if (!str) {
            return '';
        }
        return decodeURIComponent(escape(String(str)));
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

        hexlify: function(bytes) {
            var i, len = bytes.length, res = '', c1, c2;

            for (i = 0; i < len; i++) {
                c1 = bytes[i];

                c2 = (c1 & 0xf0) >> 4;
                c1 = c1 & 0xf;

                res += hex[c2] + hex[c1];
            }

            return res;
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
        },

        from_bytes: function(msg) {
            var str = '', i, len;

            for (i = 0, len = msg.length; i < len; i++) {
                str += String.fromCharCode(msg[i]);
            }

            return _utf8_decode(str);
        }
    }
})();
