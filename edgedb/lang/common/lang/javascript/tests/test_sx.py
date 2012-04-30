##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import JSFunctionalTest


class TestJSsx(JSFunctionalTest):
    def test_utils_lang_js_sx_error(self):
        '''JS
        // %from semantix.utils.lang.javascript import sx

        assert.equal(String(new sx.Error('foo')), 'sx.Error: foo');
        assert.equal(String(sx.Error), 'sx.Error');
        '''

    def test_utils_lang_js_sx_len(self):
        '''JS
        // %from semantix.utils.lang.javascript import sx

        assert.equal(sx.len([]), 0);
        assert.equal(sx.len([1]), 1);
        assert.equal(sx.len([2, []]), 2);

        assert.equal(sx.len({}), 0);
        assert.equal(sx.len({'1': '2'}), 1);

        var foo = function() {};
        foo.prototype = {'a': 'b'};
        foo = new foo();
        foo.b = 'c';
        assert.equal(sx.len(foo), 1);

        assert.raises(function() {
            sx.len(1);
        }, {
            error: sx.Error,
            error_re: 'supports only objects and arrays'
        });
        '''

    def test_utils_lang_js_sx_each(self):
        '''JS
        // %from semantix.utils.lang.javascript import sx

        assert.equal(
            (function() {
                var cnt = 0;
                sx.each([10, 20, 30], function(value, idx) {
                    cnt += idx * value;
                });
                return cnt;
            })(),

            80
        );

        assert.equal(
            (function() {
                var cnt = 0;
                sx.each({'1': 10, '2': 20, '3': 30}, function(value, idx) {
                    cnt += parseInt(idx) * value;
                });
                return cnt;
            })(),

            140
        );
        '''
