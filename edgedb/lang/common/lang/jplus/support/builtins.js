/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from metamagic.utils.lang.javascript import sx_base, class


$SXJSP = (function() {
    'use strict';

    var modules = {},
        StdObject = {}.constructor,
        StdArray = [].constructor,
        StdFunction = (function(){}).constructor,
        hop = StdObject.prototype.hasOwnProperty,
        tos = StdObject.prototype.toString,
        Array_slice = StdArray.prototype.slice,
        _no_kwarg = {}; // marker

    var Object_keys = sx.keys;

    var Function_bind = StdFunction.prototype.bind || (function (scope) {
        if (!callable(this)) {
            throw new TypeError("Function.prototype.bind - what is trying to be bound is not callable");
        }

        var args = Array_slice.call(arguments, 1),
            me = this,
            check_cls = function () {},
            bound = function () {
                return me.apply((this instanceof check_cls && scope) ? this : scope,
                                args.concat(Array_slice.call(arguments)));
            };

        check_cls.prototype = this.prototype;
        bound.prototype = new check_cls();

        return bound;
    });

    function Module(name, dct) {
        this.$name = name;
        this.$initialized = dct != null;

        for (var i in dct) {
            if (hop.call(dct, i) && i.length && i[0] != '$') {
                this[i] = dct[i];
            }
        }
    }

    function error(msg) {
        throw new TypeError('$SXJSP: ' + msg);
    }

    function is(x, y) {
        return (x === y) ? (x !== 0 || 1 / x === 1 / y) : (x !== x && y !== y);
        //                             [1]                         [2]

        // [1]: 0 === -0, but they are not identical

        // [2]: NaN !== NaN, but they are identical.
        // NaNs are the only non-reflexive value, i.e., if x !== x,
        // then x is a NaN.
        // isNaN is broken: it converts its argument to number, so
        // isNaN("foo") => true
    }

    function len(obj) {
        var t = tos.call(obj);

        if (t == '[object Array]') {
            return obj.length;
        }

        if (t == '[object Object]') {
            return Object_keys(obj).length;
        }

        if (t == '[object String]') {
            return obj.length;
        }

        throw new TypeError('object "' + obj + '" has no len()');
    }

    function isnumber(n) {
        return (typeof n == 'number' || tos.call(n) == '[object Number]')
                                                    && isFinite(n) && !isNaN(n);
    }

    function isarray(a) {
        return tos.call(a) == '[object Array]';
    }

    function isstring(s) {
        return typeof s == 'string';
    }

    function isobject(o) {
        return tos.call(o) == '[object Object]';
    }

    function abs(num) {
        if (isnumber(num)) {
            return Math.abs(num);
        }

        throw new TypeError('bad operand type for abs(): "' + num + '"');
    }

    function callable(obj) {
        return (typeof obj == 'function') || (tos.call(obj) == '[object Function]');
    }

    function pow(x, y) {
        if (!isnumber(x)) {
            throw new TypeError('bad first argument type for pow(): "' + x + '"');
        }

        if (!isnumber(y)) {
            throw new TypeError('bad second argument type for pow(): "' + y + '"');
        }

        return Math.pow(x, y);
    }

    function EXPORTS(x) { return x; } // for static analysis

    return EXPORTS({
        /* private */

        _modules: modules,

        _module: function(name, dct) {
            var parts = name.split('.'),
                i, len = parts.length,
                next = modules,
                next_sub,
                iter_name = [],
                part,
                mod,
                mod_name,
                mod_dct;

            if (!len) {
                error('invalid module name: "' +name + '"');
            }

            for (i = 0; i < len - 1; i++) {
                part = parts[i];
                iter_name.push(part);
                next_sub = next;
                if (next_sub.hasOwnProperty(part)) {
                    next = next_sub[part];
                } else {
                    next = next_sub[part] = new Module(iter_name.join('.'));
                }
            }

            mod_name = parts[len - 1];
            mod = next[mod_name];
            if (mod != null) {
                if (mod.$initialized) {
                    error('duplicate module? (' + name + ')');
                }
                for (i in dct) {
                    if (!mod.hasOwnProperty(i) && dct.hasOwnProperty(i)) {
                        mod[i] = dct[i];
                    }
                }
            } else {
                next[mod_name] = new Module(name, dct);
            }
        },

        _validate_with: function(obj) {
            if (!obj.enter || tos.call(obj.enter) != '[object Function]'
                   || !obj.exit || tos.call(obj.exit) != '[object Function]') {
                error('with: context managers must have "enter" and "exit" methods');
            }
        },

        _slice1: function(obj, n) {
            return Array_slice.call(obj, n);
        },
        _slice2: function(obj, n1, n2) {
            return Array_slice.call(obj, n1, n2);
        },
        _no_kwarg: _no_kwarg,
        _filter_kwargs: function(kwargs) {
            var result = {};
            for (var i in kwargs) {
                if (hop.call(kwargs, i) && kwargs[i] !== _no_kwarg && i != '__jpkw') {
                    result[i] = kwargs[i];
                }
            }
            return result;
        },

        _is: is,
        _isnt: function(x, y) {
            return !is(x, y);
        },

        _throw_assert_error: function(expr) {
            throw new Error(expr);
        },

        _hop: function(obj, attr) {
            return hop.call(obj, attr);
        },
        _newclass: sx.define.new_class,
        _super_method: sx.parent.find,
        _isinstance: sx.isinstance,
        _bind: Function_bind,
        __cleanup_modules: function() {
            modules = {}; // for tests only
        },
        _required_kwonly_arg_missing: function(name, arg_name) {
            throw new TypeError(name + '() needs keyword-only argument ' + arg_name);
        },
        _inv_pos_only_args: function(name, got, total) {
            throw new TypeError(name + '() takes ' + total + ' of positional only arguments ' +
                                '(' + got + ' given)');
        },
        _assert_empty_kwargs: function(name, kw) {
            if (len(kw)) {
                throw new TypeError(name + '() got an unexpected keyword argument ' +
                                    Object_keys(kw)[0]);
            }
        },

        /* public */

        keys: Object_keys,
        len: len,
        abs: abs,
        pow: pow,

        isnumber: isnumber,
        isarray: isarray,
        isstring: isstring,
        isobject: isobject,
        callable: callable,

        isinstance: sx.isinstance,
        issubclass: sx.issubclass,
        BaseObject: sx.BaseObject,
        object: sx.object,
        type: sx.type,

        print: function() {
            if (typeof print != 'undefined') {
                return print.apply(this, arguments);
            }

            if (typeof console != 'undefined' && console.log) {
                return console.log.apply(console, arguments);
            }

            error('print function is unsupported');
        }
    });
})();
