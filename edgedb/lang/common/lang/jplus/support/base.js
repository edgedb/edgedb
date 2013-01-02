/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


$SXJSP = (function() {
    'use strict';

    var modules = {};

    var hop = Object.hasOwnProperty;

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
        throw '$SXJSP: ' + msg;
    }

    return {
        module: function(name, dct) {
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
        }
    };
})();
