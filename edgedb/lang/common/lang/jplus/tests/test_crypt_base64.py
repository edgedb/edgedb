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

from jplus.tests import base as jpt_base


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
fn tobytes(msg) {
    r = []
    msg = unescape(encodeURIComponent(String(msg)))

    for i = 0; i < msg.length; i++ {
        r.push(msg.charCodeAt(i))
    }

    return r
}
'''


class TestJPCrypt(jpt_base.BaseJPlusTest):
    def test_utils_lang_jp_crypt_sha1(self):
        self.run_test(source='''
            from metamagic.utils.lang.jplus import crypt

            hashes = ''' + sha1 + tobytes + '''

            for key in hashes {
                assert hashes[key] == crypt.sha1(key)
                assert hashes[key] == crypt.sha1(tobytes(key))
            }
        ''')

    def test_utils_lang_jp_crypt_md5(self):
        self.run_test(source='''
            from metamagic.utils.lang.jplus import crypt

            hashes = ''' + md5 + tobytes + '''

            for key in hashes {
                assert hashes[key] == crypt.md5(key)
                assert hashes[key] == crypt.md5(tobytes(key))
            }
        ''')

    def test_utils_lang_jp_base64_b32_1(self):
        self.run_test(source='''
            from metamagic.utils.lang.jplus import base64

            hashes = ''' + b32 + tobytes + '''

            for key in hashes {
                assert hashes[key] == base64.b32encode(key)
                assert hashes[key] == base64.b32encode(tobytes(key))
            }
        ''')

    def test_utils_lang_jp_base64_b32_2(self):
        self.run_test(source='''
            from metamagic.utils.lang.jplus import base64, uuid

            uid = new uuid.UUID('d18651b4f95a11e2b0161b391a0272b6')
            enc = base64.b32encode(uid.toBytes())
            assert enc == '2GDFDNHZLII6FMAWDM4RUATSWY======'
        ''')

    def test_utils_lang_jp_base64_b64_1(self):
        self.run_test(source='''
            from metamagic.utils.lang.jplus import base64

            hashes = ''' + b64 + tobytes + '''

            for key in hashes {
                assert hashes[key] == base64.b64encode(key)
                assert hashes[key] == base64.b64encode(tobytes(key))
            }
        ''')

    def test_utils_lang_jp_base64_b64_2(self):
        self.run_test(source='''
            from metamagic.utils.lang.jplus import base64, uuid

            uid = new uuid.UUID('d18651b4f95a11e2b0161b391a0272b6')
            enc = base64.b64encode(uid.toBytes())
            assert enc == '0YZRtPlaEeKwFhs5GgJytg=='
        ''')

    def test_utils_lang_jp_base64_b64_3(self):
        self.run_test(source='''
            from metamagic.utils.lang.jplus import base64

            hashes = ''' + b64 + tobytes + '''

            for key in hashes {
                assert key == base64.b64decodeToStr(base64.b64encode(key))
            }
        ''')

    def test_utils_lang_jp_base64_b64_4(self):
        self.run_test(source='''
            from metamagic.utils.lang.jplus import base64, uuid

            uid = new uuid.UUID('d18651b4-f95a-11e2-b016-1b391a0272b6')
            enc = base64.b64encode(uid.toBytes());

            assert enc == '0YZRtPlaEeKwFhs5GgJytg=='

            dec = base64.b64decode(enc)
            nuid = new uuid.UUID(dec)

            assert nuid.hex == uid.hex
        ''')
