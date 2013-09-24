##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import json
import hashlib
import random
import string
import base64

from .base import JSFunctionalTest


class TestJSsxCrypt(JSFunctionalTest):
    def random_text(len):
        spec = string.ascii_letters + string.digits
        return ''.join(random.choice(spec) for i in range(len))


    sha1 = {}
    md5 = {}
    b32 = {}
    b64 = {}
    for v in ('', 'abc', '123', random_text(3), random_text(5), random_text(10),
              random_text(20), random_text(50), random_text(100),
              random_text(200), random_text(1000), random_text(9999),
              random_text(100000), '©', random_text(random.randrange(100, 100000)),
              'd18651b4f95a11e2b0161b391a0272b6'):
        sha1[v] = hashlib.sha1(v.encode('utf-8')).hexdigest()
        md5[v] = hashlib.md5(v.encode('utf-8')).hexdigest()
        b32[v] = base64.b32encode(v.encode('utf-8')).decode('ascii')
        b64[v] = base64.b64encode(v.encode('utf-8')).decode('ascii')
    sha1 = json.dumps(sha1)
    md5 = json.dumps(md5)
    b32 = json.dumps(b32)
    b64 = json.dumps(b64)

    tobytes = '''
    function tobytes(msg) {
        var r = [], msg = unescape(encodeURIComponent(String(msg)));

        for (var i = 0; i < msg.length; i++) {
            r.push(msg.charCodeAt(i));
        }

        return r;
    };
    '''

    def test_utils_lang_js_sx_crypt_sha1(self):
        pass
    test_utils_lang_js_sx_crypt_sha1.__doc__ = '''JS
    // %from metamagic.utils.lang.javascript import crypt

    var hashes = ''' + sha1 + ''';''' + tobytes + '''

    sx.each(hashes, function(hash, key) {
        assert.equal(hash, sx.crypt.sha1(key));
        assert.equal(hash, sx.crypt.sha1(tobytes(key)));
    });
    '''

    def test_utils_lang_js_sx_crypt_md5(self):
        pass
    test_utils_lang_js_sx_crypt_md5.__doc__ = '''JS
    // %from metamagic.utils.lang.javascript import crypt

    var hashes = ''' + md5 + ''';''' + tobytes + '''

    sx.each(hashes, function(hash, key) {
        assert.equal(hash, sx.crypt.md5(key));
        assert.equal(hash, sx.crypt.md5(tobytes(key)));
    });
    '''

    def test_utils_lang_js_sx_base64_b32_1(self):
        pass
    test_utils_lang_js_sx_base64_b32_1.__doc__ = '''JS
    // %from metamagic.utils.lang.javascript import base64

    var hashes = ''' + b32 + ''';''' + tobytes + '''

    sx.each(hashes, function(hash, key) {
        assert.equal(hash, sx.base64.b32encode(key));
        assert.equal(hash, sx.base64.b32encode(tobytes(key)));
    });
    '''

    def test_utils_lang_js_sx_base64_b32_2(self):
        '''JS
        // %from metamagic.utils.lang.javascript import base64, uuid

        var uid = new sx.UUID('d18651b4f95a11e2b0161b391a0272b6');
        var enc = sx.base64.b32encode(uid.toBytes());
        assert.equal(enc, '2GDFDNHZLII6FMAWDM4RUATSWY======');
        '''

    def test_utils_lang_js_sx_base64_b64_1(self):
        pass
    test_utils_lang_js_sx_base64_b64_1.__doc__ = '''JS
    // %from metamagic.utils.lang.javascript import base64

    var hashes = ''' + b64 + ''';''' + tobytes + '''

    sx.each(hashes, function(hash, key) {
        assert.equal(hash, sx.base64.b64encode(key));
        assert.equal(hash, sx.base64.b64encode(tobytes(key)));
        return;
    });
    '''

    def test_utils_lang_js_sx_base64_b64_2(self):
        '''JS
        // %from metamagic.utils.lang.javascript import base64, uuid

        var uid = new sx.UUID('d18651b4f95a11e2b0161b391a0272b6');
        var enc = sx.base64.b64encode(uid.toBytes());
        assert.equal(enc, '0YZRtPlaEeKwFhs5GgJytg==');
        '''

    def test_utils_lang_js_sx_base64_b64_3(self):
        pass
    test_utils_lang_js_sx_base64_b64_3.__doc__ = '''JS
    // %from metamagic.utils.lang.javascript import base64

    var hashes = ''' + b64 + ''';''' + tobytes + '''

    sx.each(hashes, function(hash, key) {
        assert.equal(key, sx.base64.b64decode_tostr(sx.base64.b64encode(key)));
        return;
    });
    '''

    def test_utils_lang_js_sx_base64_b64_4(self):
        '''JS
        // %from metamagic.utils.lang.javascript import base64, uuid

        var uid = new sx.UUID('d18651b4-f95a-11e2-b016-1b391a0272b6');
        var enc = sx.base64.b64encode(uid.toBytes());

        assert.equal(enc, '0YZRtPlaEeKwFhs5GgJytg==');

        var dec = sx.base64.b64decode(enc);
        var nuid = new sx.UUID(dec);

        assert.equal(nuid.hex, uid.hex);
        '''
