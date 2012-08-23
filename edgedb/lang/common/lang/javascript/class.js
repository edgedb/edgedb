/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from . import sx


(function() {
    'use strict';

    var initializing = false,
        hop = Object.hasOwnProperty,
        error_mcls_conflict = 'metaclass conflict: the metaclass of a derived class must be ' +
                              'a (non-strict) subclass of the metaclasses of all its bases';


    function mro_merge(seqs) {
        var result = [], i, j, k, s, slen, len, nothead, nonemptyseqs, seq, cand = null;

        for(;;) {
            len = seqs.length;
            nonemptyseqs = [];
            for (i = 0; i < len; i++) {
                if (seqs[i].length) {
                    nonemptyseqs.push(seqs[i]);
                }
            }

            len = nonemptyseqs.length;
            if (len == 0) {
                return result;
            }

            for (i = 0; i < len; i++) {
                seq = nonemptyseqs[i];
                cand = seq[0];

                nothead = false;
                for (j = 0; j < len; j++) {
                    s = nonemptyseqs[j];
                    slen = s.length;
                    for (k = 1; k < slen; k++) {
                        if (s[k] === cand) {
                            nothead = true;
                            break;
                        }
                    }
                    if (nothead) {
                        break;
                    }
                }

                if (nothead) {
                    cand = null;
                } else {
                    break;
                }
            }

            if (cand == null) {
                throw new sx.Error('Cannot create a consistent method resolution');
            }

            result.push(cand);

            for (i = 0; i < len; i++) {
                seq = nonemptyseqs[i]
                if (seq[0] === cand) {
                    seq.shift();
                }
            }
        }
    };

    function calc_mro(cls) {
        if (hop.call(cls, '$mro')) {
            return cls.$mro.slice();
        }

        var lst = [[cls]],
            i = 0,
            bases = cls.$bases,
            len = bases.length;

        for (; i < len; i++) {
            lst.push(calc_mro(bases[i]));
        }

        lst.push(bases.slice());

        return mro_merge(lst);
    };

    function calc_metaclasses(bases) {
        var result = [],
            i = 0,
            bases_len = bases.length,
            mcls;

        for (; i < bases_len; i++) {
            mcls = bases[i].$cls;

            if (result.indexOf(mcls) < 0) {
                result.push(mcls);
            }
        }

        return result;
    };

    function parent(cls, ths, method, args) {
        var mro, mro_len, i, pos, base, func, cls;

        args = args || [];
        if (!hop.call(arguments, 'length') || arguments.length > 4) {
            throw new sx.Error('invalid `sx.parent` call, should receive arguments as array');
        }

        if (hop.call(ths, '$mro')) {
            // class

            mro = ths.$mro;
            mro_len = mro.length;
            pos = mro.indexOf(cls);

            if (pos >= 0) {
                for (i = pos + 1; i < mro_len; i++) {
                    base = mro[i];

                    if (hop.call(base, method)) {
                        func = base[method];
                        if (hop.call(func, '$wrapped')) {
                            func = func.$wrapped;
                        }
                        return func.apply(ths, args);
                    }
                }
            }
        } else {
            mro = ths.$cls.$mro;
            mro_len = mro.length;
            pos = mro.indexOf(cls);

            if (pos >= 0) {
                for (i = pos + 1; i < mro_len; i++) {
                    base = mro[i].prototype;

                    if (hop.call(base, method)) {
                        return base[method].apply(ths, args);
                    }
                }
            }
        }

        throw new sx.Error("can't find '" + method + "' parent method for '" + cls + "'");
    };

    function make_universal_constructor(initializer) {
        return function _class() {
            var args, base, pos, mro, mro_len, i,  ths = null, func;

            if (this instanceof _class) {
                if (initializing) {
                    return;
                }

                args = (arguments.length && hop.call(arguments[0], '$sx_args'))
                                                                ? arguments[0] : arguments;

                mro = _class.$mro;
                mro_len = mro.length;
                pos = mro.indexOf(_class);

                if (pos >= 0) {
                    for (i = pos; i < mro_len; i++) {
                        base = mro[i];

                        if (hop.call(base, 'constructor')) {
                            func = base.constructor;
                            if (hop.call(func, '$wrapped')) {
                                func = func.$wrapped;
                            }
                            _class.$inst = this;
                            ths = func.apply(_class, args);
                            delete _class['$inst'];
                            break;
                        }
                    }
                }

                if (ths == null) {
                    throw new sx.Error(_class + ': could not create an instance');
                }

                if (initializer === null) {
                    if (pos >= 0) {
                        for (i = pos; i < mro_len; i++) {
                            base = mro[i].prototype;

                            if (hop.call(base, 'constructor')) {
                                base.constructor.apply(ths, args);
                                return ths;
                            }
                        }
                    }

                    return ths;
                } else {
                    initializer.apply(ths, args);
                    return ths;
                }
            } else {
                arguments.$sx_args = true;
                return new _class(arguments);
            }
        };
    };

    function boundmethod(func, scope) {
        var bound = function() {
            return func.apply(scope, arguments);
        };
        bound.$wrapped = func;
        return bound;
    };

    function new_class(name, bases, dct) {
        if (!name || !sx.is_string(name)) {
            throw new sx.Error('Empty class name');
        }

        if (!bases || !sx.is_array(bases) || bases.length == 0) {
            throw new sx.Error('Invalid `bases` for class "' + name + '"');
        }

        if (!dct || !sx.is_object(dct)) {
            throw new sx.Error('Invalid `dct` for class "' + name + '"');
        }

        var i, j, cls, parent, proto, attr,
            bases_len = bases.length, mro,
            ctr = sx.getattr(dct, 'constructor', null),
            name_parts;

        cls = make_universal_constructor(ctr);

        cls.toString = function() { return '<' + name + '>'; };

        if (name.indexOf('.') !== -1) {
            name_parts = name.split('.');
            cls.$name = name_parts[name_parts.length - 1];
            cls.$module = name_parts.slice(0, -1).join('.');
        } else {
            cls.$name = name;
            cls.$module = '';
        }

        cls.$cls = this;

        if (bases_len == 1) {
            cls.$bases = [bases[0]];
            cls.$mro = calc_mro(cls);

            initializing = true;
            proto = cls.prototype = new bases[0]();
            initializing = false;
        } else {
            cls.$bases = bases;
            cls.$mro = calc_mro(cls);
            proto = cls.prototype = {};
        }

        for (i in dct) {
            if (hop.call(dct, i) && i != 'metaclass' && i != 'statics') {
                attr = dct[i];
                if (sx.is_function(attr)) {
                    if (!hop.call(attr, '$cls')) {
                        attr.$cls = cls;
                        attr.$name = i;
                    }
                }
                proto[i] = attr;
            }
        }

        if (bases_len > 1) {
            mro = cls.$mro;

            for (i = 1; i < mro.length - 1; i++) {
                var parent = mro[i];
                for (j in parent.prototype) {
                    if (!hop.call(proto, j) && hop.call(parent.prototype, j)) {
                        proto[j] = parent.prototype[j];
                    }
                }
            }
        }

        var static_keys = {};

        if (hop.call(dct, 'statics')) {
            var statics = dct.statics;
            for (i in statics) {
                if (hop.call(statics, i)) {
                    static_keys[i] = true;
                    var s = statics[i];

                    if (sx.is_function(s)) {
                        if (!hop.call(s, '$name')) {
                            s.$name = i;
                            s.$cls = cls;
                        }

                        if (hop.call(s, '$wrapped')) {
                            s = boundmethod(s.$wrapped, cls);
                        } else {
                            s = boundmethod(s, cls);
                        }

                        s.$cls = cls;
                        s.$name = i;
                    }

                    cls[i] = s;
                }
            }
        }

        for (i = 0; i < bases_len; i++) {
            statics = bases[i].$statics;
            var st;
            for (j = 0; j < statics.length; j++) {
                st = statics[j];
                if (!hop.call(static_keys, st)) {
                    static_keys[st] = true;
                    var s = bases[i][st];

                    if (sx.is_function(s)) {
                        if (!hop.call(s, '$name')) {
                            s.$name = st;
                            s.$cls = cls;
                        }

                        if (hop.call(s, '$wrapped')) {
                            s = boundmethod(s.$wrapped, cls);
                        } else {
                            s = boundmethod(s, cls);
                        }

                        s.$cls = cls;
                        s.$name = st;
                    }

                    cls[st] = s;
                }
            }
        }

        cls.$statics = sx.keys(static_keys);

        return cls;
    };

    sx.Type = make_universal_constructor(null);
    sx.Type.constructor = new_class;
    new_class.$cls = sx.Type;
    new_class.$name = 'constructor';
    sx.Type.$statics = [];

    var object_constructor = function() {
        return this;
    };
    sx.Object = make_universal_constructor(null);
    object_constructor.$name = 'constructor';
    object_constructor.$cls = 'sx.Object';
    sx.Object.prototype = {
        constructor: object_constructor,
        toString: function() {
            return '<instance of ' + this.$cls.$name + '>';
        }
    };
    sx.Object.constructor = function() {
        var obj = this.$inst;
        obj.$cls = this;
        return obj;
    };
    sx.Object.constructor.$cls = sx.Object;
    sx.Object.constructor.$name = 'constructor';


    sx.Object.$cls = sx.Type;
    sx.Object.$statics = [];
    sx.Object.$bases = [sx.Object];
    sx.Object.$name = 'Object';
    sx.Object.$module = 'sx';
    sx.Object.$mro = [sx.Object];
    sx.Object.toString = function() { return '<sx.Object>'; };

    sx.Type.$bases = [sx.Object];
    sx.Type.$cls = sx.Type;
    sx.Type.$name = 'Type';
    sx.Object.$module = 'sx';
    sx.Type.$mro = [sx.Type, sx.Object];
    sx.Type.prototype = {
        constructor: function() {
            return this;
        }
    }
    sx.Type.prototype.constructor.$cls = sx.Type;
    sx.Type.prototype.constructor.$name = 'constructor';

    sx.Type.toString = function() { return '<sx.Type>'; };

    sx.parent = parent;

    sx.define = function(name, bases, body) {
        body = body || {};
        bases = bases || [];

        if (sx.len(bases) == 0) {
            bases = [sx.Object];
        }

        var metaclass = sx.getattr(body, 'metaclass', null);
        if (metaclass === null) {
            var ms = calc_metaclasses(bases),
                i = 0,
                ms_len = ms.length,
                j, mcls, found;

            for (i = 0; i < ms_len; i++) {
                mcls = ms[i];
                found = true;
                for (j = 0; j < ms_len; j++) {
                    if (!sx.issubclass(mcls, ms[j])) {
                        found = false;
                        break;
                    }
                }
                if (found) {
                    metaclass = mcls;
                    break;
                }
            }

            if (metaclass === null) {
                throw new sx.Error(error_mcls_conflict);
            }

        } else if (sx.issubclass(metaclass, sx.Type)) {
            var ms = calc_metaclasses(bases),
                ms_len = ms.length,
                i = 0;

            for (; i < ms_len; i++) {
                if (!sx.issubclass(metaclass, ms[i])) {
                    throw new sx.Error(error_mcls_conflict);
                }
            }
        }

        if (arguments.length > 3) {
            var args = [name, bases, body];
            args.push.apply(args, Array.prototype.slice.call(arguments, 3));
            return metaclass.apply(null, args);
        } else {
            return metaclass(name, bases, body);
        }
    };

    sx.issubclass = function(cls, parents) {
        if (!cls || !hop.call(cls, '$mro')) {
            return false;
        }

        var mro = cls.$mro;

        if (sx.is_array(parents)) {
            var i = 0, len = parents.length;
            for (; i < len; i++) {
                if (mro.indexOf(parents[i]) >= 0) {
                   return true;
                }
            }

            return false;

        } else if (hop.call(parents, '$mro')) {
            return mro.indexOf(parents) >= 0;
        }

        return false;
    };

    sx.isinstance = function(inst, clss) {
        return hop.call(inst, '$cls') && sx.issubclass(inst.$cls, clss);
    };
})();
