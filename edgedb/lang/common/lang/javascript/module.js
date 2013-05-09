/**
 * Copyright (c) 2012-2013 Sprymix Inc.
 * All rights reserved.
 *
 * See LICENSE for details.
 */


_sx_module = (function(global) {
    var cbs = {}, loaded = {};

    var module = function(name, code) {
        name = name.replace(/\:.*$/, '');

        var mod = code.call(global, name);
        if (cbs[name]) {
            for (var i = 0, c = cbs[name], len = c.length; i < len; i++) {
                c[i][0].call(c[i][1], name, mod);
            }

            delete cbs[name];
        }
        loaded[name] = mod;
    };

    module.onload = function(name, cb, scope) {
        if (loaded.hasOwnProperty(name)) {
            cb.call(scope || global, name, loaded[name]);
            return;
        }
        if (!cbs[name]) {
            cbs[name] = [];
        }
        cbs[name].push([cb, scope || global]);
    };

    module.is_loaded = function(name) {
        return loaded.hasOwnProperty(name);
    };

    return module;
})(this);
