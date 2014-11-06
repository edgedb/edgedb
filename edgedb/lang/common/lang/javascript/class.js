/*
* Copyright (c) 2012, 2014 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %import metamagic.utils.lang.javascript.sx
// %import jplus.runtime


(function() {

var StdObject = {}.constructor,
    hop = StdObject.prototype.hasOwnProperty,
    tos = StdObject.prototype.toString,
    natives = [Array, Number, Date, Boolean, String, Object, RegExp];


sx.issubclass = JPlus.issubclass;
sx.isinstance = JPlus.isinstance;

sx.objId = JPlus.id;


sx.object = JPlus.JObject;
sx.type = JPlus.JType;


sx.parent = function(cls, ths, method, args) {
    if (!args) {
        args = [];
    }

    if (method === 'constructor' && ths['__mro__'] && sx.issubclass(ths, cls)) {
        method = 'new';
    }

    if (typeof cls === 'string') {
        cls = sx.ns.resolve(cls);
    }

    var parent = JPlus._super_method(cls, ths, method);
    return parent.apply(ths, args)
}


_newFactories = {};
function _getNewFactory(n) {
    if (_newFactories[n]) {
        return _newFactories[n];
    }

    var args = [], src = [], i;
    for (i = 0; i < n; i++) {
        args.push('_' + i);
        src.push('_' + i);
    }

    src = src.join(', ');
    src = 'return new this(' + src + ')';

    _newFactories[n] = new Function(args, src);

    return _newFactories[n];
}


function __call__() {
    if (!arguments.length) {
        return new this();
    }

    var f = _getNewFactory(arguments.length);
    return f.apply(this, arguments);
}


sx.define = function(name, bases, body) {
    var statics, attr, attrname, metaclass = null, args, extra_args = false;

    if (!bases) {
        bases = [sx.object];
    }

    if (!body) {
        body = {};
    }

    body.__call__ = __call__;

    if (arguments.length > 3) {
        extra_args = true;
        args = [];
        for (var i = 3; i < arguments.length; i++) {
            args.push(arguments[i]);
        }
    }

    body['__extra_args__'] = args;

    if (hop.call(body, 'metaclass')) {
        metaclass = body['metaclass'];
        if (typeof metaclass == 'string') {
            metaclass = sx.ns.resolve(metaclass);
        }
        delete body.metaclass;
    }

    for (var i = 0; i < bases.length; i++) {
        if (typeof bases[i] === 'string') {
            bases[i] = sx.ns.resolve(bases[i]);
        }
    }

    if (hop.call(body, 'statics')) {
        statics = body.statics;

        for (attrname in statics) {
            if (!hop.call(statics, attrname)) {
                continue;
            }

            attr = statics[attrname];

            if (attrname === 'constructor') {
                attrname = 'new';
            }

            if (hop.call(body, attrname)) {
                throw new Error('duplicate static/non-static property "' +
                    attrname + '" in class "' + name + '"');
            }

            if (attrname !== 'new'
                    && tos.call(attr) === '[object Function]'
                    && natives.indexOf(attr) === -1
                    && !attr['__class__']) {
                attr = JPlus._wrap_static(attr);
            }

            if (attrname === 'new') {
                attr = (function(attr) {
                    return function(name, bases, dct) {
                        var new_args = [name, bases, dct];
                        if (dct && hop.call(dct, '__extra_args__')) {
                            new_args.push.apply(new_args, dct.__extra_args__);
                        }
                        return attr.apply(this, new_args);
                    }
                })(attr)
            }

            body[attrname] = attr;
        }
    }

    body.statics = body;

    var module = null,
        fullname = name;

    if (name.indexOf('.') > 0) {
        name = sx.str.rpartition(name, '.');
        module = name[0];
        name = name[2];
    }

    body['$$metamagic_class$$'] = true;

    var cls = JPlus._newclass(module, name, bases, body, metaclass);

    cls.statics = null;

    if (module) {
        sx.ns(fullname, cls);
    }

    return cls;
}


sx.define.forModule = function(modName) {
    return function(name, bases, dct) {
        if (name.indexOf('.') >= 0) {
            throw new Error('invalid class name "' + name + '" (remove the ".")');
        }

        var fullname = name;
        if (name[0] != '_') {
            var fullname = modName + '.' + name;
        }

        return sx.define(fullname, bases, dct);
    }
};


})();
