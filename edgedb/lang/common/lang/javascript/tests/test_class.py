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
        // %from metamagic.utils.lang.javascript import class

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

        assert.equal(Foo.__name__, 'Foo');
        assert.equal(Foo.__module__, 'test');

        assert.equal(Bar.__name__, 'Bar');
        assert.equal(Bar.__module__, 'test.sub');

        assert.equal(sx.type.__name__, 'type');
        assert.equal(sx.object.__name__, 'object');
        '''

    def test_utils_lang_js_sx_class_2(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var A = sx.define('A', [], {
            a: function() { return 1; }
        });

        var B = sx.define('B', [], {
            b: function() { return 2; }
        });

        var C = sx.define('C', [A, B], {
            c: function() { return 3; }
        });

        assert.equal(A.__name__, 'A');
        assert.equal(A.__module__, null);

        assert.equal(A.__mro__, [A, sx.object]);
        assert.equal(B.__mro__, [B, sx.object]);
        assert.equal(sx.object.__mro__, [sx.object]);
        assert.equal(C.__mro__, [C, A, B, sx.object]);

        assert.equal((new A()).a(), 1);
        assert.equal((new B()).b(), 2);

        assert.equal((new C()).a(), 1);
        assert.equal((new C()).b(), 2);
        assert.equal((new C()).c(), 3);
        '''

    def test_utils_lang_js_sx_class_3(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var F = sx.define('F');
        var E = sx.define('E');
        var D = sx.define('D');
        var C = sx.define('C', [D, F]);

        var B = sx.define('B', [D, E]);
        var A = sx.define('A', [B, C]);

        var B2 = sx.define('B2', [E, D]);
        var A2 = sx.define('A2', [B2, C]);

        var Z = sx.define('Z', [A2]);

        assert.equal(A.__mro__, [A, B, C, D, E, F, sx.object]);
        assert.equal(A2.__mro__, [A2, B2, E, C, D, F, sx.object]);

        assert.equal(F.__mro__, [F, sx.object]);
        assert.equal(C.__mro__, [C, D, F, sx.object]);

        assert.equal(Z.__mro__, [Z, A2, B2, E, C, D, F, sx.object]);
        '''

    def test_utils_lang_js_sx_class_4(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var A = sx.define('A');
        var B = sx.define('B');
        var C = sx.define('C');
        var D = sx.define('D');
        var E = sx.define('E');
        var K1 = sx.define('K1', [A, B, C]);
        var K2 = sx.define('K2', [D, B, E]);
        var K3 = sx.define('K3', [D, A]);
        var Z = sx.define('Z', [K1, K2, K3]);

        assert.equal(Z.__mro__, [Z, K1, K2, K3, D, A, B, C, E, sx.object]);
        '''

    def test_utils_lang_js_sx_class_call_parent_1(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

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
        assert.not(sx.issubclass(B, [sx.type]));
        assert.ok(sx.issubclass(B, [sx.type, A]));
        assert.ok(sx.issubclass(G, A));

        assert.ok(sx.isinstance(A(), A));
        assert.ok(sx.isinstance(B(), A));
        assert.not(sx.isinstance(A(), B));
        assert.not(sx.isinstance(B(), [sx.type]));
        assert.ok(sx.isinstance(B(), [sx.type, A]));
        assert.ok(sx.isinstance(G(), A));

        assert.equal(A.cm(), [A, 'A']);
        assert.equal(B.cm(), [B, [B, 'A'], 'B']);
        assert.equal(D.cm(), [D, [D, [D, [D, 'A'], 'B'], 'C'], 'D']);
        assert.equal(E.cm(), [E, [E, [E, [E, 'A'], 'B'], 'C'], 'D']);

        assert.equal(C.__name__, 'C');
        '''

    def test_utils_lang_js_sx_class_call_parent_2(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var chk = [];

        var ProtoObject = sx.define('ProtoObject', [], {
            constructor: function() {
                chk.push('ProtoObject');
            }
        });

        var ClassPrototype = sx.define('ClassPrototype', [ProtoObject]);

        var ProtoSource = sx.define('ProtoSource', [ClassPrototype], {
            constructor: function() {
                chk.push('ProtoSource');
                sx.parent(ProtoSource, this, 'constructor');
            }
        });

        var ProtoNode = sx.define('ProtoNode', [ClassPrototype]);

        var ProtoConcept = sx.define('ProtoConcept', [ProtoNode, ProtoSource]);

        ProtoConcept();
        assert.equal(chk, ['ProtoSource','ProtoObject']);
        '''

    def test_utils_lang_js_sx_class_no_new(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var Test = sx.define('Test', [], {
            constructor: function(v, w) {
                this.foo = v + ':' + w;
            }
        });

        assert.equal((new Test('bar', 'spam')).foo, 'bar:spam');
        assert.equal(Test('bar', 'spam').foo, 'bar:spam');
        assert.equal(Test().__class__, Test);

        var Test2 = sx.define('Test2', [], {
            constructor: function(v) {
                this.foo = v;
            }
        });

        assert.equal((new Test2('bar', 'spam')).foo, 'bar');
        assert.equal(Test2('bar', 'spam').foo, 'bar');
        assert.equal(Test2('bar').__class__, Test2);

        assert.equal((new Test2('bar')).foo, 'bar');
        assert.equal(Test2('bar').foo, 'bar');
        '''

    def test_utils_lang_js_sx_class_metaclass_1(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.type], {
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

        assert.equal(A.__class__, MA);

        assert.equal(A().foo, 'AA')
        assert.equal(A().__class__, A);

        var B = sx.define('B', [A]);
        assert.equal(B().foo, 'BB')
        assert.equal(B.__class__, MA);
        '''

    def test_utils_lang_js_sx_class_metaclass_2(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.type]);

        assert.equal(MA.__mro__, [MA, sx.type, sx.object]);

        var A = sx.define('A', [], {
            metaclass: MA
        });

        assert.equal(A.__class__, MA);
        assert.equal((new A()).__class__, A);

        var B = sx.define('B', [A]);
        assert.equal(B.__class__, MA);
        '''

    def test_utils_lang_js_sx_class_metaclass_3(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.type], {
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

        assert.equal(A.__class__, MA);
        assert.equal(A().foo, 'AA');
        assert.not(A().bar);

        assert.equal(B.__class__, MB);
        assert.equal(B().foo, 'BB');
        assert.equal(B().bar, 'BBbar');

        assert.equal(C.__class__, MB);
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
        // %from metamagic.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.type]);
        var A = sx.define('A', [], {
            metaclass: MA
        });

        var MB = sx.define('MB', [sx.type]);
        var B = sx.define('B', [], {
            metaclass: MB,

            foo: function() {
                return 42;
            }
        });

        var B1 = sx.define('B1', [B]),
            B2 = sx.define('B2', [B1]);

        assert.equal(B1.__class__, MB);
        assert.equal(B2.__class__, MB);
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
        // %from metamagic.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.type], {
            constructor: function(name, bases, dct) {
                this.baz = 42;
            },

            statics: {
                constructor: function(name, bases, dct) {
                    assert.equal(this.__name__, 'MA');
                    dct.foo = name + name;
                    var cls = sx.parent(MA, this, 'constructor', [name, bases, dct]);
                    assert.ok(sx.issubclass(cls, sx.object));
                    assert.not(sx.issubclass(cls, sx.type));
                    assert.equal(cls.__class__, MA);
                    assert.equal(cls.__name__, name);
                    assert.ok('a' in cls.prototype);
                    cls.bar = 'bar';
                    return cls;
                }
            }
        });

        var Foo = MA('Foo', [sx.object],
                         {a: function() { return this.__class__.bar; }} );

        var Bar = sx.define('Bar', [Foo], {
            constructor: function(p1, p2) {
                assert.equal(this.__class__.__name__, 'Bar');
                this.foo += p2;
            },

            statics: {
                constructor: function(p1, p2) {
                    assert.equal(this.__name__, 'Bar');
                    assert.equal(this.__class__, MA);
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
        // %from metamagic.utils.lang.javascript import class

        var Base = sx.define('Base', [], {
            foo: function() {
                return 42;
            }
        });

        var MMA = sx.define('MMA', [sx.type], {
            constructor: function() {
                this.bar = function() { return 'bar'; };
            }
        });

        var MA = sx.define('MA', [MMA], {
            statics: {
                constructor: function(name, bases, dct, foo) {
                    assert.equal(foo, 123);
                    bases.splice(0, 0, Base);
                    var cls = sx.parent(this, this, 'constructor', arguments);
                    cls.bar = this.bar;
                    return cls;
                }
            }
        });

        var A = sx.define('A', [], {
            metaclass: MA
        }, 123);

        assert.equal(A().foo(), 42);
        assert.equal(A.bar(), 'bar');
        '''

    def test_utils_lang_js_sx_class_metaclass_7(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var MA = sx.define('MA', [sx.type], {
            bar: 42,
            foo: function() {
                return this.bar;
            }
        });

        var A = sx.define('A', [], {
            metaclass: MA
        });

        var MB = sx.define('MB', [MA], {
            bar: 100
        });

        var B = sx.define('B', [A], {
            metaclass: MB
        });

        assert.equal(A.foo(), 42);
        assert.equal(B.foo(), 100);

        var C = sx.define('C', [], {
            metaclass: MB
        });

        assert.ok(sx.isinstance(C, MB));
        assert.ok(sx.isinstance(C, MA));

        assert.equal(C.foo(), 100);

        var D = sx.define('D', [B, C]);
        assert.equal(D.foo(), 100);

        var MC = sx.define('MC', [MA], {
            foo: function() {
                return sx.parent(MC, this, 'foo') + 1;
            }
        });

        var E = sx.define('E', [], {
            metaclass: MC
        });

        assert.equal(E.foo(), 43);

        var MF = sx.define('MF', [MC, MB]);

        var F = sx.define('F', [E], {
            metaclass: MF
        });

        assert.equal(F.foo(), 101);
        '''

    def test_utils_lang_js_sx_class_metaclass_8(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var chk = [];

        var ProtoObject = sx.define('ProtoObject', [sx.type], {
            constructor: function() {
                chk.push('ProtoObject');
                return sx.parent(ProtoObject, this, 'constructor');
            }
        });

        var ClassPrototype = sx.define('ClassPrototype', [ProtoObject]);

        var ProtoSource = sx.define('ProtoSource', [ClassPrototype], {
            constructor: function() {
                chk.push('ProtoSource');
                return sx.parent(ProtoSource, this, 'constructor');
            }
        });

        var ProtoNode = sx.define('ProtoNode', [ClassPrototype]);

        var ProtoConcept = sx.define('ProtoConcept', [ProtoNode, ProtoSource]);

        var cls = sx.define('cls', [], {
            metaclass: ProtoConcept,
            foo: function() {
                return 'foo';
            }
        });

        assert.equal(cls.__class__, ProtoConcept);
        assert.equal(cls.__name__, 'cls');
        assert.equal(cls().foo(), 'foo');
        assert.equal(chk, ['ProtoSource','ProtoObject']);
        '''

    def test_utils_lang_js_sx_class_metaclass_9(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var chk = [];

        var ProtoObject = sx.define('ProtoObject', [sx.type], {
            statics: {
                constructor: function(name, bases, dct) {
                    chk.push('ProtoObject');
                    return sx.parent(ProtoObject, this, 'constructor', arguments);
                }
            }
        });

        var ClassPrototype = sx.define('ClassPrototype', [ProtoObject]);

        var ProtoSource = sx.define('ProtoSource', [ClassPrototype], {
            statics: {
                constructor: function(name, bases, dct) {
                    chk.push('ProtoSource');
                    return sx.parent(ProtoSource, this, 'constructor', arguments);
                }
            }
        });

        var ProtoNode = sx.define('ProtoNode', [ClassPrototype]);

        var ProtoConcept = sx.define('ProtoConcept', [ProtoNode, ProtoSource]);

        var cls = sx.define('cls', [], {
            metaclass: ProtoConcept,
            foo: function() {
                return 'foo';
            }
        });

        assert.equal(cls.__class__, ProtoConcept);
        assert.equal(cls.__name__, 'cls');
        assert.equal(cls().foo(), 'foo');
        assert.equal(chk, ['ProtoSource','ProtoObject']);
        '''


    def test_utils_lang_js_sx_class_statics_1(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

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

    def test_utils_lang_js_sx_class_natives(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var A = sx.define('A', [], {
            date: Date,

            statics: {
                num : Number
            }
        });

        assert.ok(A.num === Number);
        assert.ok(A().date === Date);
        '''

    def test_utils_lang_js_sx_class_obj_in_obj(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var chk = [];

        var A = sx.define('A', [], {
            constructor: function(a) {
                if (a == 3) {
                    throw 'spam';
                }
                chk.push(this);
                this.a = a;
            },
            statics: {
                constructor: function(a) {
                    if (a == 1) {
                        new A(2);
                    }

                    if (a == 2) {
                        try {
                            new A(3);
                        } catch (e) {}
                    }

                    return sx.parent(A, this, 'constructor', [a]);
                }
            }
        });

        new A(1);
        assert.equal(chk.length, 2);
        assert.equal(chk[0].a, 2);
        assert.equal(chk[1].a, 1);

        chk = [];
        new A(2);
        assert.equal(chk.length, 1);
        assert.equal(chk[0].a, 2);
        '''

    def test_utils_lang_js_sx_class_instanceof_sim(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        assert.ok(sx.isinstance([], Array));
        assert.ok(sx.isinstance(new String('spam'), String));
        assert.ok(sx.isinstance(new Number(10), [Number, String]));
        assert.ok(sx.isinstance('spam', String));
        assert.ok(sx.isinstance(10, Number));
        assert.ok(sx.isinstance(10, sx.BaseObject));
        assert.ok(sx.isinstance(true, Boolean));
        assert.ok(sx.isinstance(false, Boolean));

        assert.not(sx.isinstance(null, String));
        assert.not(sx.isinstance(null, sx.BaseObject));
        assert.not(sx.isinstance(void(0), sx.BaseObject));

        var a = function(){};
        var b = function(){};
        b.prototype = new a();
        assert.ok(sx.isinstance(new b(), a));
        assert.not(sx.isinstance(b(), a));
        assert.not(sx.isinstance(8, a));
        '''

    def test_utils_lang_js_sx_class_null_constructoror_arg(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var Foo = sx.define('Foo', [], {
            constructor: function(a) {
                this.a = a;
            }
        });

        var f = Foo(null);
        assert.ok(f.a === null);

        f = new Foo(null);
        assert.ok(f.a === null);

        f = new Foo(void(0));
        assert.ok(typeof f.a === 'undefined');

        f = Foo(void(0));
        assert.ok(typeof f.a === 'undefined');
        '''

    def test_utils_lang_js_sx_class_ns(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        sx.define('com.acme.Foo', [], {
            foo: function() {return 10}
        });
        assert.equal(com.acme.Foo.__module__, 'com.acme');
        assert.equal(com.acme.Foo.__name__, 'Foo');
        assert.ok(sx.isinstance(com.acme.Foo, sx.type));

        sx.define('com.acme.BarMeta', ['sx.type']);
        sx.define('com.acme.Bar', ['com.acme.Foo', sx.object], {
            metaclass: 'com.acme.BarMeta',
            foo: function() {
                return 32 + sx.parent('com.acme.Bar', this, 'foo');
            }
        });
        assert.ok(sx.issubclass(com.acme.Bar, com.acme.Foo));
        assert.ok(sx.isinstance(com.acme.Bar, com.acme.BarMeta));
        assert.equal(com.acme.Bar().foo(), 42);
        '''

    def test_utils_lang_js_sx_class_to_string(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        sx.define('com.acme.Bar');
        assert.equal(com.acme.Bar() + '', '<instance of com.acme.Bar>');
        assert.equal(com.acme.Bar + '', '<class com.acme.Bar>');
        assert.equal(sx.type + '', '<class type>');
        assert.equal(sx.object + '', '<class object>');
        '''

    def test_utils_lang_js_sx_class_statics_scope(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        sx.define('com.acme.Bar', [], {
            statics: {
                a: 100,

                foo: function() {
                    return this.a;
                }
            }
        });

        var scope = {a: 42};

        assert.equal(com.acme.Bar.foo.call(scope), 42);
        assert.equal(com.acme.Bar.foo(), 100);
        '''

    def test_utils_lang_js_sx_class_null_attr(self):
        '''JS
        // %from metamagic.utils.lang.javascript import class

        var Foo = sx.define('Foo', [], {
            foo: null,
            bar: void 0,

            statics: {
                foo: null,
                bar: void 0
            }
        });

        assert.equal(Foo.foo, null);
        assert.equal(Foo.bar, void 0);
        assert.ok(Foo.hasOwnProperty('bar'));
        assert.equal(Foo().foo, null);
        assert.equal(Foo().bar, void 0);
        '''
