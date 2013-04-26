/*
* Copyright (c) 2012, 2013 Sprymix Inc.
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
    }

    function to_bytes(str) {
        var str = utf8_encode(str),
            len = str.length,
            i,
            res = new Array(len);
        for (i = 0; i < len; i++) {
            res[i] = str.charCodeAt(i);
        }
        return res;
    }

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

    var pads = [[0x80]],
        i,
        maxlen = 0xFFFFFFFF / 8;

    for (i = 1; i < 70; i++) {
        pads[i] = pads[i - 1].concat([0]);
    }

    var sha1 = (function() {
        // Algorithm from http://en.wikipedia.org/wiki/SHA-1

        return function sha1(msg) {
            if (msg.length > maxlen) {
                throw new Error('unable to calculate sha1: message too long');
            }

            if (sx.is_string(msg)) {
                msg = to_bytes(msg);
            }

            var h0 = 0x67452301,
                h1 = 0xEFCDAB89,
                h2 = 0x98BADCFE,
                h3 = 0x10325476,
                h4 = 0xC3D2E1F0,
                len = msg.length,
                a, b, c, d, e, i, j, k, n, tmp,
                w = Array(80);

            // pad with zeroes to be congruent to 56 (448 bits, mod 512)
            // + 4 bytes for zeroes, as part of 64bit length (we append
            // only 32 bits later)
            msg.push.apply(msg, pads[((55 - len) % 64 + 64) % 64 + 4]);

            len = len << 3; // length in bits -- len * 8
            // append 32 bit length !! big-endian !!
            msg.push((len >> 24) & 0xFF, (len >> 16) & 0xFF,
                     (len >> 8) & 0xFF, len & 0xFF);

            len = msg.length;

            for (n = 0; n < len; n += 64) {
                a = h0;
                b = h1;
                c = h2;
                d = h3;
                e = h4;

                for (i = j = 0, k = n; j < 16; i += 4, j++, k = n + i) {
                    w[j] = (msg[k] << 24) | (msg[k + 1] << 16) |
                           (msg[k + 2] << 8) | (msg[k + 3] << 0);
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

                h0 = (h0 + a) << 0;
                h1 = (h1 + b) << 0;
                h2 = (h2 + c) << 0;
                h3 = (h3 + d) << 0;
                h4 = (h4 + e) << 0;
            }

            return hex(h0) + hex(h1) + hex(h2) + hex(h3) + hex(h4);
        };
    })();

    var md5 = (function() {
        // Algorithm from http://en.wikipedia.org/wiki/MD5
        var S = [ 7, 12, 17, 22,  7, 12, 17, 22,  7, 12, 17, 22,  7, 12, 17, 22,
                  5,  9, 14, 20,  5,  9, 14, 20,  5,  9, 14, 20,  5,  9, 14, 20,
                  4, 11, 16, 23,  4, 11, 16, 23,  4, 11, 16, 23,  4, 11, 16, 23,
                  6, 10, 15, 21,  6, 10, 15, 21,  6, 10, 15, 21,  6, 10, 15, 21 ],

            K = new Array(64),

            i;

        function leftrotate(x, c) {
            return (x << c) | (x >>> (32 - c));
        }

        function reverse(num) {
            var res = ((num >>> 24) & 0xFF);
            res += ((num >>> 16) & 0xFF) << 8;
            res += ((num >>> 8) & 0xFF) << 16;
            res += (num & 0xFF) << 24;
            return res;
        }

        for (i = 0; i < 64; i++) {
            K[i] = Math.floor(Math.abs(Math.sin(i + 1)) * 0x100000000);
        }

        return function md5(msg) {
            if (msg.length > maxlen) {
                throw new Error('unable to calculate md5: message too long');
            }

            if (sx.is_string(msg)) {
                msg = to_bytes(msg);
            }

            var a0 = 0x67452301,
                b0 = 0xEFCDAB89,
                c0 = 0x98BADCFE,
                d0 = 0x10325476,
                len = msg.length,
                n, i, j, k, a, b, c, d, g, f, d_temp,
                m = new Array(16);

            // pad with zeroes to be congruent to 52 (448 bits, mod 512)
            msg.push.apply(msg, pads[((55 - len) % 64 + 64) % 64]);

            len = len << 3; // length in bits -- len * 8
            // append 32 bit length !! little-endian !!
            msg.push(len & 0xFF, (len >> 8) & 0xFF,
                     (len >> 16) & 0xFF, (len >> 24) & 0xFF,
                     0, 0, 0, 0);

            len = msg.length;

            for (n = 0; n < len; n += 64) {
                for (i = j = 0, k = n; j < 16; i += 4, j++, k = n + i) {
                    // little endian
                    m[j] = (msg[k] << 0) | (msg[k + 1] << 8) |
                           (msg[k + 2] << 16) | (msg[k + 3] << 24);
                }

                a = a0;
                b = b0;
                c = c0;
                d = d0;

                for (i = 0; i < 16; i++) {
                    f = (b & c) | ((~b) & d);
                    d_temp = d;
                    d = c;
                    c = b;
                    b = (b + leftrotate((a + f + K[i] + m[i]), S[i])) << 0;
                    a = d_temp;
                }

                for (i = 16; i < 32; i++) {
                    f = (d & b) | ((~d) & c);
                    d_temp = d;
                    d = c;
                    c = b;
                    b = (b + leftrotate(a + f + K[i] + m[(5 * i + 1) % 16], S[i])) << 0;
                    a = d_temp;
                }

                for (i = 32; i < 48; i++) {
                    f = b ^ c ^ d;
                    d_temp = d;
                    d = c;
                    c = b;
                    b = (b + leftrotate(a + f + K[i] + m[(3 * i + 5) % 16], S[i])) << 0;
                    a = d_temp;
                }

                for (i = 48; i < 64; i++) {
                    f = c ^ (b | (~d));
                    d_temp = d;
                    d = c;
                    c = b;
                    b = (b + leftrotate(a + f + K[i] + m[(7 * i) % 16], S[i])) << 0;
                    a = d_temp;
                }

                a0 = (a + a0) << 0;
                b0 = (b + b0) << 0;
                c0 = (c + c0) << 0;
                d0 = (d + d0) << 0;
            }

            return hex(reverse(a0)) + hex(reverse(b0)) + hex(reverse(c0)) + hex(reverse(d0));
        };
    })();

    return {
        sha1: sha1,
        md5: md5
    };
})();
