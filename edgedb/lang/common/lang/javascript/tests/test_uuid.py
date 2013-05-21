##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import JSFunctionalTest


class TestJSuuid(JSFunctionalTest):
    def test_utils_lang_js_uuid_1(self):
        '''JS

        // %from metamagic.utils.lang.javascript import uuid

        var u = new sx.UUID('ca8adeb4-e3ec-4510-bef9-a933085c19f6');

        assert.equal(u.toHex(), 'ca8adeb4e3ec4510bef9a933085c19f6');
        assert.ok((u + '') == 'ca8adeb4-e3ec-4510-bef9-a933085c19f6');
        assert.ok(u == 'ca8adeb4-e3ec-4510-bef9-a933085c19f6');
        assert.equal(u.toBytes(), [202, 138, 222, 180, 227, 236, 69, 16, 190, 249,
                                   169, 51, 8, 92, 25, 246]);
        '''

    def test_utils_lang_js_uuid_2(self):
        '''JS

        // %from metamagic.utils.lang.javascript import uuid

        assert.not(sx.UUID.uuid4() + '' == sx.UUID.uuid4() + '');

        // Check that our uuid4 matches the UUID spec (from wikipedia)
        var r = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx';
        r = r.replace('y', '[89ab]').replace(/x/g, '[a-f0-9]');
        r = '^' + r + '$';
        r = new RegExp(r);
        assert.not('f8f6df46-c22f-11e2-8c16-7c6d62900c7c'.match(r));
        assert.ok(sx.UUID.uuid4().toString().match(r));
        '''
