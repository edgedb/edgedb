##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import JSFunctionalTest


class TestJSsx(JSFunctionalTest):
    def test_utils_lang_js_sx_date_parse_iso(self):
        '''JS

        // %from metamagic.utils.lang.javascript import sx

        var d = sx.date.parse_iso('2012-01-01T15:50:00+04:00');
        assert.equal(d.toUTCString(), 'Sun, 01 Jan 2012 11:50:00 GMT');

        d = sx.date.parse_iso('2012-01-01 15:50:00+04:00');
        assert.equal(d.toUTCString(), 'Sun, 01 Jan 2012 11:50:00 GMT');

        d = sx.date.parse_iso('2012-01-01 15:50:00+04');
        assert.equal(d.toUTCString(), 'Sun, 01 Jan 2012 11:50:00 GMT');

        d = sx.date.parse_iso('2012-09-15 15:38:52.9147-04');
        assert.equal(d.toUTCString(), 'Sat, 15 Sep 2012 19:38:52 GMT');

        // Zulu, a.k.a. UTC
        d = sx.date.parse_iso('2012-09-15 15:38:52.9147Z');
        assert.equal(d.toUTCString(), 'Sat, 15 Sep 2012 15:38:52 GMT');

        // Basic date format
        d = sx.date.parse_iso('2012-09-15 155000.914113+0400');
        assert.equal(d.toUTCString(), 'Sat, 15 Sep 2012 11:50:00 GMT');

        // Truncated
        d = sx.date.parse_iso('2012-09-15 15:20+0400');
        assert.equal(d.toUTCString(), 'Sat, 15 Sep 2012 11:20:00 GMT');

        // Even more truncated
        d = sx.date.parse_iso('2012-09-15T15+0400');
        assert.equal(d.toUTCString(), 'Sat, 15 Sep 2012 11:00:00 GMT');

        d = sx.date.parse_iso('2012-09-15T15Z');
        assert.equal(d.toUTCString(), 'Sat, 15 Sep 2012 15:00:00 GMT');

        d = sx.date.parse_iso('2012-09-15');
        assert.equal(d.toUTCString(), 'Sat, 15 Sep 2012 00:00:00 GMT');

        d = sx.date.parse_iso('2012-09');
        assert.equal(d.toUTCString(), 'Sat, 01 Sep 2012 00:00:00 GMT');

        d = sx.date.parse_iso('2012');
        assert.equal(d.toUTCString(), 'Sun, 01 Jan 2012 00:00:00 GMT');

        // Before epoch
        d = sx.date.parse_iso('1904-02-29 15:00-04:00');
        assert.equal(d.toUTCString(), 'Mon, 29 Feb 1904 19:00:00 GMT');
        '''

    def test_utils_lang_js_sx_ns(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        sx.ns('com.acme.foo');
        assert.ok(com.acme.foo);
        assert.not(com.acme.bar);
        assert.not(sx.len(com.acme.foo));

        assert.raises(function() {
            sx.ns('com.acme.foo', 10);
        }, {error: sx.Error,
            error_re: 'conflicting namespace'});

        sx.ns('com.acme.bar', 42);
        assert.equal(com.acme.bar, 42);

        assert.equal(sx.ns.resolve('com.acme.bar'), 42);
        assert.equal(sx.ns.resolve('com.acme.baz', 10), 10);

        assert.raises(function() {
            sx.ns.resolve('com.acme.baz');
        }, {error: sx.Error,
            error_re: 'unable to resolve'});

        assert.raises(function() {
            sx.ns.resolve('spam');
        }, {error: sx.Error,
            error_re: 'unable to resolve'});

        var foo = {'bar': 10};
        assert.equal(sx.ns.resolve_from(foo, 'bar'), 10);
        assert.equal(sx.ns.resolve_from(foo, 'baz.spam', 42), 42);
        assert.raises(function() {
            sx.ns.resolve_from(foo, 'spam');
        }, {error: sx.Error,
            error_re: 'unable to resolve'});
        '''

    def test_utils_lang_js_sx_is(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.ok(sx.is_string('aaa'));
        assert.ok(sx.is_string(''));
        assert.not(sx.is_string(123));
        assert.not(sx.is_string(null));
        assert.not(sx.is_string(void(0)));
        assert.not(sx.is_string({}));

        assert.ok(sx.is_array([]));
        assert.ok(sx.is_array([1,2]));
        assert.not(sx.is_array(123));
        assert.not(sx.is_array({}));
        assert.not(sx.is_array(''));
        assert.not(sx.is_array(void(0)));

        assert.ok(sx.is_object({}));
        assert.ok(sx.is_object(new (function(){})));
        assert.not(sx.is_object(123));
        assert.not(sx.is_object([]));
        assert.not(sx.is_object(null));
        assert.not(sx.is_object(''));
        assert.not(sx.is_object(void(0)));
        '''

    def test_utils_lang_js_sx_json_parse(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.equal(sx.json.parse('{"a":42}'), {'a': 42});
        '''

    def test_utils_lang_js_sx_error(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.equal(String(new sx.Error('foo')), 'sx.Error: foo');
        assert.equal(String(sx.Error), 'sx.Error');
        '''

    def test_utils_lang_js_sx_contains(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.ok(sx.contains('foobar', 'ob'))
        assert.not(sx.contains('foobar', 'bo'))
        assert.not(sx.contains('', 'ob'))
        assert.raises(function() {
            sx.contains('', 10);
        }, {error: sx.Error,
            error_re: 'expected string'});

        assert.ok(sx.contains({'a': 40}, 'a'))
        assert.not(sx.contains({'a': 40}, 'b'))
        assert.raises(function() {
            sx.contains({}, 10);
        }, {error: sx.Error,
            error_re: 'only strings'});

        assert.ok(sx.contains([10, 20], 10))
        assert.not(sx.contains([10, 20], 50))
        assert.not(sx.contains([10, 20], '10'))
        assert.not(sx.contains([], '10'))
        '''

    def test_utils_lang_js_sx_hasattr(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.ok(sx.hasattr({'a': 'b'}, 'a'));
        assert.not(sx.hasattr({}, 'hasOwnProperty'));

        // ----

        var foo = function() {};
        foo.prototype = {
            'a': 'b',
            'hasOwnProperty': function() {
                return true;
            }
        };
        foo = new foo();
        foo.b = 'c';

        assert.ok(sx.hasattr(foo, 'b'));
        assert.ok(sx.hasattr(foo, 'a', true)); // weak

        assert.not(sx.hasattr(foo, 'a'));
        assert.not(sx.hasattr(foo, 'hasOwnProperty'));
        assert.not(sx.hasattr(foo, 'c', true)); // weak

        assert.not(sx.hasattr(null, 'a'));
        assert.not(sx.hasattr(void(0), 'a'));
        '''

    def test_utils_lang_js_sx_first(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.ok(sx.first([1, 2, 3]) === 1);
        assert.ok(sx.first({'a': 'b', 'c': 'd'}) === 'b');
        assert.ok(sx.first('xyz') === 'x');

        assert.ok(sx.first([], 42) === 42);
        assert.ok(sx.first({}, 42) === 42);
        assert.ok(sx.first('', 42) === 42);

        assert.raises(function() {
            sx.first(1);
        }, {error: sx.Error,
            error_re: 'supports only arrays and objects'});

        assert.raises(function() {
            sx.first({});
        }, {error: sx.Error,
            error_re: 'empty object passed with no default'});

        assert.raises(function() {
            sx.first([]);
        }, {error: sx.Error,
            error_re: 'empty array passed with no default'});

        assert.raises(function() {
            sx.first('');
        }, {error: sx.Error,
            error_re: 'empty string passed with no default'});
        '''

    def test_utils_lang_js_sx_len(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

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

        assert.equal(sx.len(''), 0);
        assert.equal(sx.len('123'), 3);
        '''

    def test_utils_lang_js_sx_each(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.equal(
            (function() {
                var cnt = 0;
                sx.each([], function(value, idx) {
                    cnt += idx * value;
                });
                return cnt;
            })(),

            0
        );

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
                sx.each([10, 20, 30], function(value) {
                    cnt += value;
                });
                return cnt;
            })(),

            60
        );

        assert.equal(
            (function() {
                var cnt = 0;
                sx.each([10, 20, 30], function(value, idx, desc) {
                    cnt += idx * value;

                    if (value === 10 && idx === 0 && desc.first) {
                        cnt += 100;
                    }
                    else if (value === 30 && idx === 2 && desc.last) {
                        cnt += 1000;
                    } else {
                        if (desc.first) {
                            cnt += 0.1;
                        }

                        if (desc.last) {
                            cnt += 0.2;
                        }
                    }
                });
                return cnt;
            })(),

            1180
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

        assert.equal(
            (function() {
                var cnt = 0;
                sx.each({'1': 10, '2': 20, '3': 30}, function(value) {
                    cnt += value;
                });
                return cnt;
            })(),

            60
        );

        assert.equal(
            (function() {
                var cnt = 0;
                sx.each({}, function(value) {
                    cnt += value;
                });
                return cnt;
            })(),

            0
        );

        assert.equal(
            (function() {
                var cnt = 0;
                sx.each({'1': 10, '2': 20, '3': 30}, function(value, idx, desc) {
                    cnt += parseInt(idx) * value;

                    if (value === 10 && idx === '1' && desc.first) {
                        cnt += 100;
                    }
                    else if (value === 30 && idx === '3' && desc.last) {
                        cnt += 1000;
                    } else {
                        if (desc.first) {
                            cnt += 0.1;
                        }

                        if (desc.last) {
                            cnt += 0.2;
                        }
                    }
                });
                return cnt;
            })(),

            1240
        );
        '''

    def test_utils_lang_js_sx_id(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.ok(sx.id() !== sx.id());
        '''

    def test_utils_lang_js_sx_partial(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        function foo(a, b, c) {
            a = a || 1;
            b = b || 10;
            c = c || 100;
            return a + b + c;
        }

        var p1 = sx.partial(foo, this);
        assert.equal(p1(), 111);
        assert.equal(p1(), 111);
        assert.equal(sx.partial(foo, this, 2)(), 112);
        assert.equal(sx.partial(foo, this, 2, 20)(), 122);
        assert.equal(sx.partial(foo, this, 2, 20, 200)(), 222);
        assert.equal(sx.partial(foo, this, 2, 20, 200, 3000)(), 222);

        var bar = {a: 'spam'};

        function baz(hello) {
            hello = hello || 'hello, ';
            return hello + this.a;
        }

        assert.equal(sx.partial(baz, bar)(), 'hello, spam');
        assert.equal(sx.partial(baz, bar, 'hi, ')(), 'hi, spam');
        '''

    def test_utils_lang_js_sx_apply(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        var a = {'c': 'd'}, b = {'a': 'b'};
        var c = sx.apply(a, b);

        assert.equal(a['a'], 'b');
        assert.equal(a['c'], 'd');
        assert.equal(sx.len(a), 2);

        a = {'a': 1}; b = {'a': 2};
        sx.apply(a, b);
        assert.equal(a['a'], 2);
        assert.equal(sx.len(a), 1);

        a = {};
        sx.apply(a, {'a': 1}, {'a': 2}, {'a': 3});
        assert.equal(a, {'a': 3});
        '''

    def test_utils_lang_js_sx_str(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        assert.equal(sx.str.trim('aaa'), 'aaa');
        assert.equal(sx.str.trim('a a  a'), 'a a  a');
        assert.equal(sx.str.trim('  a a  a'), 'a a  a');
        assert.equal(sx.str.trim('a a  a   '), 'a a  a');
        assert.equal(sx.str.trim(   'a a  a   '), 'a a  a');

        assert.ok(sx.str.endswith('aaa', 'a'));
        assert.ok(sx.str.endswith('a', 'a'));
        assert.ok(sx.str.endswith('abc', 'bc'));
        assert.ok(sx.str.endswith('abc', ''));
        assert.ok(sx.str.endswith('', ''));
        assert.not(sx.str.endswith('1abcbc', 'abc'));
        assert.not(sx.str.endswith('abc', 'b'));
        assert.not(sx.str.endswith('', 'b'));

        assert.ok(sx.str.startswith('aaa', 'a'));
        assert.ok(sx.str.startswith('a', 'a'));
        assert.ok(sx.str.startswith('abc', 'ab'));
        assert.ok(sx.str.startswith('abc', ''));
        assert.ok(sx.str.startswith('', ''));
        assert.not(sx.str.startswith('abc', 'b'));
        assert.not(sx.str.startswith('', 'b'));

        assert.equal(sx.str.rpartition('this is the rpartition method', 'ti'),
                     ['this is the rparti', 'ti', 'on method']);
        var s = 'http://www.sprymix.com';
        assert.equal(sx.str.rpartition(s, '://'), ['http', '://', 'www.sprymix.com']);
        assert.equal(sx.str.rpartition(s, '?'), ['', '', 'http://www.sprymix.com']);
        assert.equal(sx.str.rpartition(s, 'http://'), ['', 'http://', 'www.sprymix.com']);
        assert.equal(sx.str.rpartition(s, 'com'), ['http://www.sprymix.', 'com', '']);

        assert.raises(function() {
            sx.str.rpartition(s, '');
        }, {error: sx.Error,
            error_re: 'empty separator'});
        '''

    def test_utils_lang_js_sx_array_insort(self):
        '''JS
        // %from metamagic.utils.lang.javascript import sx

        var a1 = [],
            a2 = [],
            digits = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            f;

        for (var i = 0; i < 50; i++) {
            var digit = sx.random.choice(digits);
            if (sx.contains([0, 2, 4, 6, 8], digit)) {
                f = sx.array.insort_left;
            } else {
                f = sx.array.insort_right;
            }

            f(a1, digit);
            a2.push(digit);
        }

        a2.sort();
        assert.equal(a1, a2);
        '''
