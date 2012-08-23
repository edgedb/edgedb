/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from . import sx


sx.crypt = (function() {
    'use strict';

    function utf8_encode(str) {
        if (!str) {
            return '';
        }
        return unescape(encodeURIComponent(String(str)));
    };

    function hex(n) {
        return (n >> 28 & 0xF).toString(16) +
               (n >> 24 & 0xF).toString(16) +
               (n >> 20 & 0xF).toString(16) +
               (n >> 16 & 0xF).toString(16) +
               (n >> 12 & 0xF).toString(16) +
               (n >> 8 & 0xF).toString(16) +
               (n >> 4 & 0xF).toString(16) +
               (n & 0xF).toString(16)
    }

    var sha1 = (function() {
        // Algorithm from http://en.wikipedia.org/wiki/SHA-1

        var fcc = String.fromCharCode,
            pad0 = fcc(0),
            pads = [fcc(0x80), fcc(0x80) + pad0],
            i,
            maxlen = 0xFFFFFFFF / 8;

        for (i = 2; i < 65; i++) {
            pads[i] = pads[i - 1] + fcc(0);
        }

        return function sha1(msg) {
            if (msg.length > maxlen) {
                throw new sx.Error('unable to calculate sha1: message too long');
            }

            msg = utf8_encode(msg);

            var h0 = 0x67452301,
                h1 = 0xEFCDAB89,
                h2 = 0x98BADCFE,
                h3 = 0x10325476,
                h4 = 0xC3D2E1F0,
                len = msg.length,
                a, b, c, d, e, i, j, k, n, tmp,
                w = Array(80);

            msg += pads[((59 - len) % 64 + 64) % 64]; // fix JS 'mod' bugs

            len = len << 3; // length in bits -- len * 8
            msg += fcc((len >> 24) & 0xFF) + fcc((len >> 16) & 0xFF) +
                   fcc((len >> 8) & 0xFF) + fcc(len & 0xFF);

            len = msg.length;

            for (n = 0; n < len; n += 64) {
                a = h0;
                b = h1;
                c = h2;
                d = h3;
                e = h4;

                for (i = j = 0, k = n; j < 16; i += 4, j++, k = n + i) {
                    w[j] = (msg.charCodeAt(k) << 24) | (msg.charCodeAt(k + 1) << 16) |
                           (msg.charCodeAt(k + 2) << 8) | (msg.charCodeAt(k + 3));
                }

                for (i = 16; i < 80; i++) {
                    k = w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16];
                    w[i] = (k << 1) | (k >>> 31);
                }

                for (i = 0; i < 20; i++) {
                    tmp = ((a << 5) | (a >>> 27)) + e + w[i] + 0x5A827999 + ((b & c) | (~b & d));
                    e = d;
                    d = c;
                    c = (b << 30) | (b >>> 2);
                    b = a;
                    a = tmp;
                }

                for (i = 20; i < 40; i++) {
                    tmp = ((a << 5) | (a >>> 27)) + e  + w[i] + 0x6ED9EBA1 + (b ^ c ^ d);
                    e = d;
                    d = c;
                    c = (b << 30) | (b >>> 2);
                    b = a;
                    a = tmp;
                }

                for (i = 40; i < 60; i++) {
                    tmp = ((a << 5) | (a >>> 27)) + e  + w[i] + 0x8F1BBCDC +
                                                                ((b & c) | (b & d) | (c & d));
                    e = d;
                    d = c;
                    c = (b << 30) | (b >>> 2);
                    b = a;
                    a = tmp;
                }

                for (i = 60; i < 80; i++) {
                    tmp = ((a << 5) | (a >>> 27)) + e + w[i] + 0xCA62C1D6 + (b ^ c ^ d);
                    e = d;
                    d = c;
                    c = (b << 30) | (b >>> 2);
                    b = a;
                    a = tmp;
                }

                h0 = (h0 + a) & 0xFFFFFFFF;
                h1 = (h1 + b) & 0xFFFFFFFF;
                h2 = (h2 + c) & 0xFFFFFFFF;
                h3 = (h3 + d) & 0xFFFFFFFF;
                h4 = (h4 + e) & 0xFFFFFFFF;
            }

            return hex(h0) + hex(h1) + hex(h2) + hex(h3) + hex(h4);
        };
    })();

    return {
        sha1: sha1
    };
})();
