##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from . import base as base_test


class TestBuiltins(base_test.BaseJPlusTest):
    def test_utils_lang_jp_builtins_len_1(self):
        '''JS+

        print(len({'a':'b'}) + '-' + len([1, 2, 3, 4]) + '-' + len('123'))

        try {
            len(null)
        } except (TypeError) {
            print('ok')
        } else {
            print('fail')
        }

        %%
        1-4-3\nok
        '''

    def test_utils_lang_jp_builtins_abs_1(self):
        '''JS+

        print(abs(-1) + abs(2.2))

        try {
            abs('foo')
        } except (TypeError) {
            print('ok')
        } else {
            print('fail') // Math.abs would return NaN
        }

        %%
        3.2\nok
        '''

    def test_utils_lang_jp_builtins_isnumber_1(self):
        '''JS+

        print(isnumber(1) + '|' + isnumber('aa') + '|' + isnumber(' ') +
              '|' + isnumber(Infinity) + '|' + isnumber(1.2) + '|' + isnumber('\t') +
              '|' + isnumber(NaN) + '|' + isnumber('1') + '|' + isnumber(new Number(1)))

        %%
        true|false|false|false|true|false|false|false|true
        '''

    def test_utils_lang_jp_builtins_isarray_1(self):
        '''JS+

        print(isarray(1) + '|' + isarray('aa') + '|' + isarray({}) +
              '|' + isarray([]))

        %%
        false|false|false|true
        '''

    def test_utils_lang_jp_builtins_isobject_1(self):
        '''JS+

        print(isobject(1) + '|' + isobject('aa') + '|' + isobject({}) +
              '|' + isobject([]) + '|' + isobject(new (function() {})))

        %%
        false|false|true|false|true
        '''

    def test_utils_lang_jp_builtins_isstring_1(self):
        '''JS+

        print(isstring(1) + '|' + isstring('aa') + '|' + isstring({}) +
              '|' + isstring([]) + '|' + isstring(String('sdf')))

        %%
        false|true|false|false|true
        '''

    def test_utils_lang_jp_builtins_callable_1(self):
        '''JS+

        print(callable(1) + '|' + callable(function(){}) + '|' +
              callable({}) + '|' + callable(len) + '|' +
              callable(new function() {}) + '|' + callable(Object) + '|' +
              callable(type))

        %%
        false|true|false|true|false|true|true
        '''

    def test_utils_lang_jp_builtins_pow_1(self):
        '''JS+

        print(pow(5.0, 2))

        try {
            pow('1 ', 2)
        } except (TypeError) {
            print('ok')
        } else {
            print('fail')
        }

        try {
            pow(1, '2 ')
        } except (TypeError) {
            print('ok')
        } else {
            print('fail')
        }

        %%
        25\nok\nok
        '''
