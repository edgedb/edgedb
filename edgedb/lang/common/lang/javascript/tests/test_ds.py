##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import JSFunctionalTest


class TestJSds(JSFunctionalTest):
    def test_utils_lang_js_ds_map(self):
        '''JS

        // %from metamagic.utils.lang.javascript import sx, ds, class

        var m = new sx.ds.Map(),
            undefined = void(0);

        assert.equal(m.size, 0);

        var k = {};
        m.set(k, 'foobar');
        assert.equal(m.get(k), 'foobar');
        assert.equal(m.size, 1);
        assert.ok(m.has(k));

        assert.equal(m.get({}), undefined);
        assert.not(m.has({}));

        m.set('123', 123);
        assert.equal(m.get('123'), 123);
        assert.equal(m.size, 2);

        m.set(456, '456');
        assert.equal(m.get(456), '456');
        assert.equal(m.size, 3);
        assert.ok(m.has(456));
        assert.not(m.has(111111));

        m.set(null, 'null');
        assert.equal(m.get(null), 'null');
        assert.equal(m.size, 4);

        m.set(undefined, 'undef');
        assert.equal(m.get(undefined), 'undef');
        assert.equal(m.size, 5);

        var f = function() {};
        m.set(f, 42);
        assert.equal(m.get(f), 42);
        assert.equal(m.size, 6);
        assert.equal(m.get(function(){}), undefined);

        var A = sx.define('A', [], {});
        assert.not(A.$hash);
        m.set(A, A);
        assert.equal(m.get(A), A);
        assert.ok(A.$hash);
        assert.equal(m.size, 7);
        var B = sx.define('A', [], {});
        assert.equal(m.get(B), undefined);
        assert.ok(m.has(A));
        assert.not(m.has(B));

        assert.not(m.del('1234'));
        assert.equal(m.size, 7);
        assert.ok(m.del('123'));
        assert.equal(m.size, 6);
        assert.equal(m.get('123'), undefined);

        assert.not(m.del(function(){}));
        assert.equal(m.size, 6);
        assert.ok(m.del(f));
        assert.equal(m.size, 5);
        assert.equal(m.get(f), undefined);

        m.clear();
        assert.equal(m.size, 0);
        assert.equal(m.get(k), undefined);
        assert.equal(m.get(A), undefined);
        assert.equal(m.get(B), undefined);
        assert.equal(m.get(456), undefined);

        assert.not(m.has(NaN));
        m.set(NaN, 123);
        assert.equal(m.get(NaN), 123);
        assert.ok(m.has(NaN));
        '''

    def test_utils_lang_js_ds_set(self):
        '''JS

        // %from metamagic.utils.lang.javascript import sx, ds, class

        var m = new sx.ds.Set(),
            undefined = void(0);

        assert.equal(m.size, 0);

        var k = {};
        m.add(k);
        assert.ok(m.has(k));
        assert.equal(m.size, 1);
        assert.not(m.has({}));

        assert.not(m.has('123'));
        m.add('123', 123);
        assert.equal(m.size, 2);
        assert.ok(m.has('123'));

        m.add(null, 'null');
        assert.ok(m.has(null));
        assert.not(m.has(undefined));
        assert.equal(m.size, 3);

        m.add(undefined);
        assert.ok(m.has(undefined));
        assert.equal(m.size, 4);
        m.del(undefined);
        assert.not(m.has(undefined));
        assert.ok(m.has(null));
        assert.equal(m.size, 3);

        var A = sx.define('A', [], {});
        assert.not(A.$hash);
        m.add(A);
        assert.ok(m.has(A));
        assert.ok(A.$hash);
        assert.equal(m.size, 4);
        var B = sx.define('A', [], {});
        assert.not(m.has(B));
        assert.ok(m.has(A));
        '''
