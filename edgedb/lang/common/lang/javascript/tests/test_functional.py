##
# Copyright (c) 2012 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import JSFunctionalTest


class TestJSFunctionalTestEngine(JSFunctionalTest):
    def test_utils_lang_js_functional_assert_raises(self):
        '''JS
        var Ex = function(msg) {this.msg = msg;}
        Ex.prototype = {
            toString: function() {
                return 'Ex: ' + this.msg;
            }
        };

        var er = false;
        try {
            assert.raises(function() {}, {
                error: Ex
            });
        } catch (e) {
            if (e instanceof assert.AssertionError) {
                er = true;
            }
        }
        if (!er) {
            throw new Error("assert.throws failed");
        }

        assert.raises(function() {
            throw new Ex();
        }, {
            error: Ex
        });

        assert.raises(function() {
            assert.raises(function() {
                throw new Ex('bar');
            }, {
                error: Ex,
                error_re: 'foo'
            });
        }, {
            error: assert.AssertionError
        });
        '''

    def test_utils_lang_js_functional_assert_equal(self):
        '''JS
        assert.equal(1, 1);
        assert.equal('', '');
        assert.equal('a', 'a');
        assert.equal('a', 'a', true);

        assert.equal('', 0, true); // weak

        assert.raises(function() {
            assert.equal('', 0);
        }, {error: assert.AssertionError});

        assert.equal([], []);
        assert.equal([1], [1]);

        assert.equal([{'a': 'b'}], [{'a': 'b'}]);

        assert.raises(function() {
            assert.equal([1], [2])
        }, {error: assert.AssertionError});

        var a = [],
            b = a;
        assert.equal(a, b);
        '''

    def test_utils_lang_js_functional_assert_ok(self):
        '''JS
        assert.ok('1');
        assert.ok(1);
        assert.ok(function(){});

        assert.raises(function() {
            assert.ok(0);
        }, {error: assert.AssertionError});

        assert.raises(function() {
            assert.ok('');
        }, {error: assert.AssertionError});

        assert.raises(function() {
            assert.ok(null);
        }, {error: assert.AssertionError});

        assert.raises(function() {
            assert.ok(false);
        }, {error: assert.AssertionError});
        '''

    def test_utils_lang_js_functional_assert_not(self):
        '''JS
        assert.not('');
        assert.not(0);
        assert.not(false);

        assert.raises(function() {
            assert.not(1);
        }, {error: assert.AssertionError});

        assert.raises(function() {
            assert.not(true);
        }, {error: assert.AssertionError});

        assert.raises(function() {
            assert.not({});
        }, {error: assert.AssertionError});

        assert.raises(function() {
            assert.not([]);
        }, {error: assert.AssertionError});
        '''
