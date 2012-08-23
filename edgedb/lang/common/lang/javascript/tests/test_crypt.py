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

from .base import JSFunctionalTest


class TestJSsxCrypt(JSFunctionalTest):
    def random_text(len):
        spec = string.ascii_letters + string.digits
        return ''.join(random.choice(spec) for i in range(len))


    hashes = {}
    for v in ('', 'abc', '123', random_text(3), random_text(5), random_text(10),
              random_text(20), random_text(50), random_text(100),
              random_text(200), random_text(1000), random_text(9999),
              random_text(100000), '©', random_text(random.randrange(100, 100000))):
        hashes[v] = hashlib.sha1(v.encode('utf-8')).hexdigest()
    hashes = json.dumps(hashes)


    def test_utils_lang_js_sx_crypt_1(self):
        pass
    test_utils_lang_js_sx_crypt_1.__doc__ = '''JS
    // %from semantix.utils.lang.javascript import crypt

    var hashes = ''' + hashes + ''';

    sx.each(hashes, function(hash, key) {
        assert.equal(hash, sx.crypt.sha1(key));
    });
    '''
