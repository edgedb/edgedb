/*
* Copyright (c) 2014 MagicStack Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


(function(global) {
    'use strict';

    var undefined = void 0,
        _id_counter = 0,
        has_own_property = Object.prototype.hasOwnProperty,
        namespace_id = '9ea3fca6-db9a-11e3-aeaf-cf59603505b2';

    if (global.sx && global.sx.$ns == namespace_id) {
        return; // same module, loaded twice
    }

    var sx = function() {
        return sx.$call.apply(this, arguments);
    };

    sx.$call = function() {
        throw 'not implemented';
    };

    sx.$ns = namespace_id;

    sx.$oldsx = global.sx;
    global.sx = sx;

    sx.apply = function sx_apply(obj /*, ... */) {
        var i = 1,
            len = arguments.length,
            property, arg;

        for (; i < len; i++) {
            arg = arguments[i];
            for (property in arg) {
                if (has_own_property.call(arg, property)) {
                    obj[property] = arg[property];
                }
            }
        }

        return obj;
    };

    sx.apply(sx, {
        Error: Error,

        id: function(suffix) {
            return 'sx-id-' + (++_id_counter) + (suffix || '');
        },

        ns: (function() {
            var ns = function(ns, obj) {
                var i, chunks = ns.split('.'),
                    len = chunks.length,
                    chunk,
                    cursor = global;

                if (obj !== undefined) {
                    len--;
                }

                for (i = 0; i < len; i++) {
                    chunk = chunks[i];
                    if (!has_own_property.call(cursor, chunk)) {
                        cursor[chunk] = {};
                    }

                    cursor = cursor[chunk];
                }

                if (obj !== undefined) {
                    chunk = chunks[i];

                    if (has_own_property.call(cursor, chunk)) {
                        throw new Error('sx.ns: conflicting namespace: "' + ns + '"');
                    }

                    cursor[chunk] = obj;
                }

                return obj;
            };

            var r = ns.resolve_from = function(root, ns, deflt) {
                var i, chunks = ns.split('.'),
                    len = chunks.length,
                    cursor = root,
                    chunk;

                for (i = 0; i < len; i++) {
                    chunk = chunks[i];
                    cursor = cursor[chunk];
                    if (cursor == undefined) {
                        if (deflt !== undefined) {
                            return deflt;
                        }
                        throw new Error('unable to resolve ns "' + ns + '"');
                    }
                }

                return cursor;
            };

            ns.resolve = function(ns, deflt) {
                return r(global, ns, deflt);
            };

            return ns;
        })(),

        keys: (function() {
            if (Object.keys) {
                return Object.keys;
            }

            // copied from
            // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/keys
            var hasDontEnumBug = !({toString: null}).propertyIsEnumerable('toString'),
                dontEnums = [
                    'toString',
                    'toLocaleString',
                    'valueOf',
                    'hasOwnProperty',
                    'isPrototypeOf',
                    'propertyIsEnumerable',
                    'constructor'
                ],
                dontEnumsLength = dontEnums.length;

            return function sx_keys(obj) {
                if (typeof obj !== 'object' && (typeof obj !== 'function' || obj === null)) {
                    throw new TypeError('Object.keys called on non-object');
                }

                var result = [], prop, i;

                for (prop in obj) {
                    if (has_own_property.call(obj, prop)) {
                        result.push(prop);
                    }
                }

                if (hasDontEnumBug) {
                    for (i = 0; i < dontEnumsLength; i++) {
                        if (has_own_property.call(obj, dontEnums[i])) {
                            result.push(dontEnums[i]);
                        }
                    }
                }

                return result;
            }
        })()
    })

})(this);
