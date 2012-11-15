/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from metamagic.utils.lang.javascript import sx, class


$SXJSP = (function() {
    'use strict';

    var modules = {};

    var StdObject = {}.constructor,
        StdArray = [].constructor,
        hop = StdObject.prototype.hasOwnProperty,
        tos = StdObject.prototype.toString;


    var Object_keys = StdObject.keys || (function () {
        // Code from:
        // https://developer.mozilla.org/en-US/docs/JavaScript/Reference/Global_Objects/Object/keys
        var hasDontEnumBug = !{toString:null}.propertyIsEnumerable("toString"),
            DontEnums = [
                'toString',
                'toLocaleString',
                'valueOf',
                'hasOwnProperty',
                'isPrototypeOf',
                'propertyIsEnumerable',
                'constructor'
            ],
            DontEnumsLength = DontEnums.length;

        return function (o) {
            if (typeof o != "object" && typeof o != "function" || o === null)
                throw new TypeError("Object.keys called on a non-object");

            var result = [];
            for (var name in o) {
                if (hop.call(o, name))
                    result.push(name);
            }

            if (hasDontEnumBug) {
                for (var i = 0; i < DontEnumsLength; i++) {
                    if (hop.call(o, DontEnums[i]))
                        result.push(DontEnums[i]);
                }
            }

            return result;
        };
    })();

    var Array_slice = StdArray.prototype.slice;

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
        // From StackOverflow answer by Christian C. Salvadó
        return !isNaN(parseFloat(n)) && isFinite(n);
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

    function EXPORTS(x) { return x; } // for static analysis

    return EXPORTS({
        /* private */

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

        _slice1: function(obj, num) {
            return Array_slice.call(obj, num);
        },

        _is: is,
        _isnt: function(x, y) {
            return !is(x, y);
        },

        _newclass: sx.define.new_class,
        _super_method: sx.parent.find,
        _isinstance: sx.isinstance,

        /* public */

        keys: Object_keys,
        len: len,
        abs: abs,

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
