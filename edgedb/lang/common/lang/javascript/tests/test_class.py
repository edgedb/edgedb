##
# Copyright (c) 2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from .base import JSFunctionalTest


class TestJSClass(JSFunctionalTest):
    def test_utils_lang_js_sx_class_1(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var Foo = sx.define('test.Foo', [], {
            foo: function() { return 1; }
        });

        var Bar = sx.define('test.sub.Bar', [Foo], {
            bar: function() { return this.foo() + 2; }
        });

        var foo = new Foo();
        assert.equal(foo.foo(), 1);

        var bar = new Bar();
        assert.equal(bar.bar(), 3);

        assert.ok(bar instanceof Foo);
        assert.ok(foo instanceof Foo);
        assert.ok(bar instanceof Bar);
        assert.not(foo instanceof Bar);

        assert.equal(Foo.$name, 'Foo');
        assert.equal(Foo.$module, 'test');

        assert.equal(Bar.$name, 'Bar');
        assert.equal(Bar.$module, 'test.sub');
        '''

    def test_utils_lang_js_sx_class_2(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var A = sx.define('A', [], {
            a: function() { return 1; }
        });

        var B = sx.define('B', [], {
            b: function() { return 2; }
        });

        var C = sx.define('C', [A, B], {
            c: function() { return 3; }
        });

        assert.equal(A.$name, 'A');
        assert.equal(A.$module, '');

        assert.equal(A.$mro, [A, sx.Object]);
        assert.equal(B.$mro, [B, sx.Object]);
        assert.equal(sx.Object.$mro, [sx.Object]);
        assert.equal(C.$mro, [C, A, B, sx.Object]);

        assert.equal((new A()).a(), 1);
        assert.equal((new B()).b(), 2);

        assert.equal((new C()).a(), 1);
        assert.equal((new C()).b(), 2);
        assert.equal((new C()).c(), 3);
        '''

    def test_utils_lang_js_sx_class_3(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var F = sx.define('F');
        var E = sx.define('E');
        var D = sx.define('D');
        var C = sx.define('C', [D, F]);

        var B = sx.define('B', [D, E]);
        var A = sx.define('A', [B, C]);

        var B2 = sx.define('B2', [E, D]);
        var A2 = sx.define('A2', [B2, C]);

        var Z = sx.define('Z', [A2]);

        assert.equal(A.$mro, [A, B, C, D, E, F, sx.Object]);
        assert.equal(A2.$mro, [A2, B2, E, C, D, F, sx.Object]);

        assert.equal(F.$mro, [F, sx.Object]);
        assert.equal(C.$mro, [C, D, F, sx.Object]);

        assert.equal(Z.$mro, [Z, A2, B2, E, C, D, F, sx.Object]);
        '''

    def test_utils_lang_js_sx_class_4(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var A = sx.define('A');
        var B = sx.define('B');
        var C = sx.define('C');
        var D = sx.define('D');
        var E = sx.define('E');
        var K1 = sx.define('K1', [A, B, C]);
        var K2 = sx.define('K2', [D, B, E]);
        var K3 = sx.define('K3', [D, A]);
        var Z = sx.define('Z', [K1, K2, K3]);

        assert.equal(Z.$mro, [Z, K1, K2, K3, D, A, B, C, E, sx.Object]);
        '''

    def test_utils_lang_js_sx_class_call_parent(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var A = sx.define('A', [], {
            constructor: function() {
                this.field = 'A';
            },

            f: function() {
                return 'A';
            },

            statics: {
                cm: function() {
                    return [this, 'A'];
                }
            }
        });

        var B = sx.define('B', [A], {
            constructor: function() {
                sx.parent(B, this, 'constructor');
                this.field += 'B';
            },

            f: function() {
                return sx.parent(B, this, 'f') + 'B';
            },

            statics: {
                cm: function() {
                    return [this, sx.parent(B, this, 'cm'), 'B'];
                }
            }
        });

        var C = sx.define('C', [A], {
            constructor: function() {
                sx.parent(C, this, 'constructor');
                this.field += 'C';
            },

            f: function() {
                return sx.parent(C, this, 'f') + 'C';
            },

            statics: {
                cm: function() {
                    return [this, sx.parent(C, this, 'cm'), 'C'];
                }
            }
        });

        var D = sx.define('D', [C, B], {
            constructor: function() {
                sx.parent(D, this, 'constructor');
                this.field += 'D';
            },

            f: function() {
                return sx.parent(D, this, 'f') + 'D';
            },

            statics: {
                cm: function() {
                    return [this, sx.parent(D, this, 'cm'), 'D'];
                }
            }
        });

        var E = sx.define('E', [D]);

        var E2 = sx.define('E2', [E]);

        var F = sx.define('F', [E], {
            f: E.prototype.f
        });

        var G = sx.define('F', [E], {
            f: function() {
                return this.field + ':' + sx.parent(G, this, 'f');
            }
        });

        assert.equal((new D).f(), 'ABCD');
        assert.equal((new E).f(), 'ABCD');
        assert.equal((new E2).f(), 'ABCD');
        assert.equal((new F).f(), 'ABCD');

        assert.equal((new D).field, 'ABCD');
        assert.equal((new E).field, 'ABCD');
        assert.equal((new E2).field, 'ABCD');
        assert.equal((new F).field, 'ABCD');

        assert.equal((new G).f(), 'ABCD:ABCD');

        assert.ok(sx.issubclass(A, A));
        assert.ok(sx.issubclass(B, A));
        assert.not(sx.issubclass(A, B));
        assert.not(sx.issubclass(B, [sx.Type]));
        assert.ok(sx.issubclass(B, [sx.Type, A]));
        assert.ok(sx.issubclass(G, A));

        assert.ok(sx.isinstance(A(), A));
        assert.ok(sx.isinstance(B(), A));
        assert.not(sx.isinstance(A(), B));
        assert.not(sx.isinstance(B(), [sx.Type]));
        assert.ok(sx.isinstance(B(), [sx.Type, A]));
        assert.ok(sx.isinstance(G(), A));

        assert.equal(A.cm(), [A, 'A']);
        assert.equal(B.cm(), [B, [B, 'A'], 'B']);
        assert.equal(D.cm(), [D, [D, [D, [D, 'A'], 'B'], 'C'], 'D']);
        assert.equal(E.cm(), [E, [E, [E, [E, 'A'], 'B'], 'C'], 'D']);
        '''

    def test_utils_lang_js_sx_class_no_new(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var Test = sx.define('Test', [], {
            constructor: function(v, w) {
                this.foo = v + ':' + w;
            }
        });

        assert.equal((new Test('bar', 'spam')).foo, 'bar:spam');
        assert.equal(Test('bar', 'spam').foo, 'bar:spam');
        assert.equal(Test().$cls, Test);

        var Test2 = sx.define('Test2', [], {
            constructor: function(v) {
                this.foo = v;
            }
        });

        assert.equal((new Test2('bar', 'spam')).foo, 'bar');
        assert.equal(Test2('bar', 'spam').foo, 'bar');
        assert.equal(Test2('bar').$cls, Test2);

        assert.equal((new Test2('bar')).foo, 'bar');
        assert.equal(Test2('bar').foo, 'bar');
        '''

    def test_utils_lang_js_sx_class_metaclass_1(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.Type], {
            statics: {
                constructor: function(name, bases, dct) {
                    dct.foo = name + name;
                    return sx.parent(MA, this, 'constructor', [name, bases, dct]);
                }
            }
        });

        var A = sx.define('A', [], {
            metaclass: MA
        });

        assert.equal(A.$cls, MA);

        assert.equal(A().foo, 'AA')
        assert.equal(A().$cls, A);

        var B = sx.define('B', [A]);
        assert.equal(B().foo, 'BB')
        assert.equal(B.$cls, MA);
        '''

    def test_utils_lang_js_sx_class_metaclass_2(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.Type]);

        assert.equal(MA.$mro, [MA, sx.Type, sx.Object]);

        var A = sx.define('A', [], {
            metaclass: MA
        });

        assert.equal(A.$cls, MA);
        assert.equal((new A()).$cls, A);

        var B = sx.define('B', [A]);
        assert.equal(B.$cls, MA);
        '''

    def test_utils_lang_js_sx_class_metaclass_3(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.Type], {
            statics: {
                constructor: function(name, bases, dct) {
                    dct.foo = name + name;
                    return sx.parent(MA, this, 'constructor', [name, bases, dct]);
                }
            }
        });

        var A = sx.define('A', [], {
            metaclass: MA
        });

        var MB = sx.define('MB', [MA], {
            statics: {
                constructor: function(name, bases, dct) {
                    dct.bar = name + name + 'bar';
                    return sx.parent(MB, this, 'constructor', [name, bases, dct]);
                }
            }
        });

        var B = sx.define('B', [A], {
            metaclass: MB
        });

        var C = sx.define('C', [B, A]);

        assert.equal(A.$cls, MA);
        assert.equal(A().foo, 'AA');
        assert.not(A().bar);

        assert.equal(B.$cls, MB);
        assert.equal(B().foo, 'BB');
        assert.equal(B().bar, 'BBbar');

        assert.equal(C.$cls, MB);
        assert.equal(C().foo, 'CC');
        assert.equal(C().bar, 'CCbar');

        assert.raises(function() {
            sx.define('D', [A, B]);
        }, {error: sx.Error,
            error_re: 'consistent method resolution'}
        )
        '''

    def test_utils_lang_js_sx_class_metaclsss_4(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.Type]);
        var A = sx.define('A', [], {
            metaclass: MA
        });

        var MB = sx.define('MB', [sx.Type]);
        var B = sx.define('B', [], {
            metaclass: MB,

            foo: function() {
                return 42;
            }
        });

        var B1 = sx.define('B1', [B]),
            B2 = sx.define('B2', [B1]);

        assert.equal(B1.$cls, MB);
        assert.equal(B2.$cls, MB);
        assert.equal(B2().foo(), 42);

        assert.raises(function() {
            sx.define('C', [B, A]);
        }, {error: sx.Error,
            error_re: 'metaclass conflict'}
        );

        assert.raises(function() {
            sx.define('C', [B, A], {
                metaclass: MB
            });
        }, {error: sx.Error,
            error_re: 'metaclass conflict'}
        );

        var C = sx.define('C', [B, A], {
            metaclass: function(name, bases, dct) {
                return '-' + name;
            }
        });
        assert.equal(C, '-C');
        '''

    def test_utils_lang_js_sx_class_metaclass_5(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.Type], {
            constructor: function(name, bases, dct) {
                this.baz = 42;
            },

            statics: {
                constructor: function(name, bases, dct) {
                    assert.equal(this.$name, 'MA');
                    dct.foo = name + name;
                    var cls = sx.parent(MA, this, 'constructor', [name, bases, dct]);
                    assert.ok(sx.issubclass(cls, sx.Object));
                    assert.not(sx.issubclass(cls, sx.Type));
                    assert.equal(cls.$cls, MA);
                    assert.equal(cls.$name, name);
                    assert.ok('a' in cls.prototype);
                    cls.bar = 'bar';
                    return cls;
                }
            }
        });

        var Foo = MA('Foo', [sx.Object],
                         {a: function() { return this.$cls.bar; }} );

        var Bar = sx.define('Bar', [Foo], {
            constructor: function(p1, p2) {
                assert.equal(this.$cls.$name, 'Bar');
                this.foo += p2;
            },

            statics: {
                constructor: function(p1, p2) {
                    assert.equal(this.$name, 'Bar');
                    assert.equal(this.$cls, MA);
                    var obj = sx.parent(this, this, 'constructor', arguments);
                    obj.foo += p1;
                    return obj;
                }
            }
        });

        var bar = new Bar(1, 2);
        assert.equal(bar.foo, 'BarBar12');
        assert.equal(Bar.bar, 'bar');
        assert.equal(bar.a(), 'bar');
        assert.equal(Bar.baz, 42);
        '''

    def test_utils_lang_js_sx_class_metaclass_6(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var Base = sx.define('Base', [], {
            foo: function() {
                return 42;
            }
        });

        var MMA = sx.define('MMA', [sx.Type], {
            constructor: function() {
                this.bar = function() { return 'bar'; };
            }
        });

        var MA = sx.define('MA', [MMA], {
            statics: {
                constructor: function(name, bases, dct) {
                    bases.splice(0, 0, Base);
                    var cls = sx.parent(this, this, 'constructor', arguments);
                    cls.bar = this.bar;
                    return cls;
                }
            }
        });

        var A = sx.define('A', [], {
            metaclass: MA
        });

        assert.equal(A().foo(), 42);
        assert.equal(A.bar(), 'bar');
        '''

    def _test_utils_lang_js_sx_class_metaclass_7(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.Type], {
            foo: function() {
                return 42;
            }
        });

        var A = sx.define('A', [], {
            metaclass: MA
        });

        print(A.foo());

        '''

    def test_utils_lang_js_sx_class_statics_1(self):
        '''JS
        // %from semantix.utils.lang.javascript import class

        var A = sx.define('A', [], {
            statics: {
                foo: 'bar',
                bar: function() {
                    return this.foo;
                }
            }
        });

        var B = sx.define('B', [], {
            statics: {
                foo: 'spam',
                baz: 'foo'
            }
        });

        var C1 = sx.define('C1', [A, B]);
        var C2 = sx.define('C2', [B, A]);

        assert.equal(A.foo, 'bar');
        assert.equal(A.bar(), 'bar');

        assert.equal(B.foo, 'spam');
        assert.equal(B.baz, 'foo');

        assert.equal(C1.bar(), 'bar');
        assert.equal(C2.bar(), 'spam');
        '''
