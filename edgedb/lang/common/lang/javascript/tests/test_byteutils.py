##
# Copyright (c) 2013 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import JSFunctionalTest


class TestJSbyteutils(JSFunctionalTest):
    def test_utils_lang_js_byteutils_1(self):
        '''JS

        // %from metamagic.utils.lang.javascript import byteutils

        assert.equal(sx.byteutils.unhexlify('ff'), [255]);
        assert.equal(sx.byteutils.unhexlify('aaff'), [170, 255]);

        assert.raises(
            function() {
                sx.byteutils.unhexlify('f');
            },
            {error: Error, error_re: 'Odd-length'}
        );
        '''

    def test_utils_lang_js_byteutils_2(self):
        '''JS
        // %from metamagic.utils.lang.javascript import byteutils

        var str = 'Δ, Й, ק, م, ๗, あ, 叶, 葉, and 말';
        assert.equal(sx.byteutils.to_bytes(str), [206, 148, 44, 32, 208, 153, 44, 32, 215, 167, 44,
                                                  32, 217, 133, 44, 32, 224, 185, 151, 44, 32, 227,
                                                  129, 130, 44, 32, 229, 143, 182, 44, 32, 232, 145,
                                                  137, 44, 32, 97, 110, 100, 32, 235, 167, 144]);

        str = 'abcd';
        assert.equal(sx.byteutils.to_bytes(str), [97, 98, 99, 100]);
        '''
