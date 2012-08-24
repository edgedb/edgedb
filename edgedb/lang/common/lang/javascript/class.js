/*
* Copyright (c) 2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from . import sx


(function() {
    'use strict';

    var instances = [],
        hop = {}.constructor.prototype.hasOwnProperty,
        tos = {}.constructor.prototype.toString,
        slice = instances.constructor.prototype.slice,
        indexOf = instances.constructor.prototype.indexOf,

        error_mcls_conflict = 'metaclass conflict: the metaclass of a derived class must be ' +
                              'a (non-strict) subclass of the metaclasses of all its bases',

        Error = sx.Error,
        natives = [Array, Number, Date, Boolean, String, Object, RegExp];

    if (!indexOf) {
        // IE
        indexOf = function(el) {
            var i = 0, len = this.length;
            for (; i < len; ++i) {
                if (this[i] === el) {
                    return i;
                }
            }
            return -1;
        }
    }

    function mro_merge(seqs) {
        var result = [], i, j, k, s, slen, len, nothead, nonemptyseqs, seq, cand = null;

        for(;;) {
            len = seqs.length;
            nonemptyseqs = [];
            for (i = 0; i < len; ++i) {
                if (seqs[i].length) {
                    nonemptyseqs.push(seqs[i]);
                }
            }

            len = nonemptyseqs.length;
            if (len == 0) {
                return result;
            }

            for (i = 0; i < len; ++i) {
                seq = nonemptyseqs[i];
                cand = seq[0];

                nothead = false;
                for (j = 0; j < len; ++j) {
                    s = nonemptyseqs[j];
                    slen = s.length;
                    for (k = 1; k < slen; ++k) {
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
                throw new Error('Cannot create a consistent method resolution');
            }

            result.push(cand);

            for (i = 0; i < len; ++i) {
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

        for (; i < len; ++i) {
            lst.push(calc_mro(bases[i]));
        }

        lst.push(bases.slice());

        return mro_merge(lst);
    };

    function calc_metaclasses(bases) {
        var result,
            i,
            bases_len = bases.length,
            mcls;

        if (bases_len == 1) {
            return [bases[0].$cls];
        }

        result = [];
        for (i = 0; i < bases_len; ++i) {
            mcls = bases[i].$cls;

            if (indexOf.call(result, mcls) < 0) {
                result.push(mcls);
            }
        }
        return result;
    };

    function parent(cls, ths, method, args) {
        var mro, mro_len, i, pos, base, func, cls;

        args = args || [];
        if (!hop.call(arguments, 'length') || arguments.length > 4) {
            throw new Error('invalid `sx.parent` call, should receive arguments as array');
        }

        if (hop.call(ths, '$mro') && !issubclass(ths.$cls, cls)) {
            // class

            mro = ths.$mro;
            pos = indexOf.call(mro, cls);

            if (pos >= 0) {
                for (i = pos + 1; base = mro[i]; ++i) {
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
            pos = indexOf.call(mro, cls);

            if (pos >= 0) {
                mro_len = mro.length;
                for (i = pos + 1; i < mro_len; ++i) {
                    base = mro[i].prototype;

                    if (hop.call(base, method)) {
                        return base[method].apply(ths, args);
                    }
                }
            }
        }

        throw new Error("can't find '" + method + "' parent method for '" + cls + "'");
    };

    function make_universal_constructor(initializer) {
        // For compatibility with IE prior 9 version, we create a '_class' variable explicitly
        var _class = function _class() {
            var args, base, pos, mro, mro_len, i,  ths = null, func;

            if (this instanceof _class) {
                args = (arguments.length && hop.call(arguments[0], '$sx_args'))
                                                                ? arguments[0] : arguments;

                mro = _class.$mro;
                mro_len = mro.length;
                for (i = 0; base = mro[i]; ++i) {
                    if (hop.call(base, 'construct')) {
                        func = base.construct;
                        if (hop.call(func, '$wrapped')) {
                            func = func.$wrapped;
                        }
                        instances.push(this);
                        try {
                            ths = func.apply(_class, args);
                        } finally {
                            if (instances[instances.length-1] === this) {
                                instances.pop();
                            }
                        }
                        break;
                    }
                }

                if (ths == null) {
                    throw new Error(_class + ': could not create an instance');
                }

                if (initializer == null) {
                    mro_len--;
                    for (i = 0; i < mro_len; ++i) { // skip self, hence 'i' from 1
                        base = mro[i].prototype;

                        if (hop.call(base, 'construct')) {
                            base.construct.apply(ths, args);
                            return ths;
                        }
                    }

                    return ths;
                } else {
                    initializer.apply(ths, args);
                    return ths;
                }
            } else {
                arguments.$sx_args = 1;
                return new _class(arguments);
            }
        };
        return _class;
    };

    function boundmethod(func, scope) {
        var bound = function() {
            return func.apply(scope, arguments);
        };
        bound.$wrapped = func;
        return bound;
    };

    function is_method(obj) {
        return (typeof obj == 'function') && !hop.call(obj, '$mro')
                                            && indexOf.call(natives, obj) == -1;
    };

    function fix_static_method(attr, static_name, cls) {
        if (!hop.call(attr, '$name')) {
            attr.$name = static_name;
            attr.$cls = cls;
        }

        if (hop.call(attr, '$wrapped')) {
            attr = boundmethod(attr.$wrapped, cls);
        } else {
            attr = boundmethod(attr, cls);
        }

        attr.$cls = cls;
        attr.$name = static_name;

        return attr;
    };

    function new_class(name, bases, dct) {
        var i, j, cls, parent, proto, attr,
            mro, attrs_flag = 0, parent_proto, mro_len_1,
            static_attrs_cache = {}, static_attrs = [],
            statics, static_name, own = [], parent_own, parent_mro;

        cls = make_universal_constructor(hop.call(dct, 'construct') ? dct.construct : null);

        cls.toString = function() { return '<' + name + '>'; };

        if ((i = name.lastIndexOf('.')) !== -1) {
            cls.$name = name.substr(i + 1);
            cls.$module = name.substr(0, i);
        } else {
            cls.$name = name;
            cls.$module = '';
        }

        cls.$cls = this;
        proto = cls.prototype = {};
        cls.$bases = bases;
        mro = cls.$mro = calc_mro(cls);
        mro_len_1 = mro.length - 1;

        for (i in dct) {
            if (hop.call(dct, i) && i != 'metaclass' && i != 'statics') {
                attr = dct[i];
                if (!hop.call(attr, '$cls') && is_method(attr)) {
                    attr.$cls = cls;
                    attr.$name = i;
                }
                proto[i] = attr;
                own.push(i);
            }
        }

        cls.$own = (attrs_flag = own.length) ? own : false;

        for (i = 1; i < mro_len_1; ++i) {
            j = mro[i];
            parent_proto = j.prototype;
            parent_own = j.$own;

            if (parent_own) {
                for (j = parent_own.length; j--;) {
                    attr = parent_own[j];
                    if (!hop.call(proto, attr)) {
                        proto[attr] = parent_proto[attr];
                        (attr != 'construct') && (attrs_flag = 1);
                    }
                }
            }
        }

        attrs_flag && (cls.$_attrs = 1);

        if (hop.call(dct, 'statics')) {
            statics = dct.statics;
            for (static_name in statics) {
                if (hop.call(statics, static_name)) {
                    static_attrs_cache[static_name] = true;
                    static_attrs.push(static_name);
                    attr = statics[static_name];

                    if (is_method(attr)) {
                        fix_static_method(attr, static_name, cls);
                    }

                    cls[static_name] = attr;
                }
            }
        }

        for (i = 1; i < mro_len_1; ++i) {
            parent_mro = mro[i];
            statics = parent_mro.$statics;
            if (statics) {
                for (j = statics.length; j--;) {
                    static_name = statics[j];
                    if (!hop.call(static_attrs_cache, static_name) && static_name != 'construct') {
                        static_attrs_cache[static_name] = true;
                        static_attrs.push(static_name);
                        attr = parent_mro[static_name];

                        if (is_method(attr)) {
                            fix_static_method(attr, static_name, cls);
                        }

                        cls[static_name] = attr;
                    }
                }
            }
        }

        if (hop.call(this, '$_attrs')) {
            parent_proto = this.prototype;
            for (static_name in parent_proto) {
                if (hop.call(parent_proto, static_name) && static_name != 'construct'
                                                            && !hop.call(cls, static_name)) {
                    attr = parent_proto[static_name];

                    if (is_method(attr)) {
                        fix_static_method(attr, static_name, cls);
                    }

                    cls[static_name] = attr;
                }
            }
        }

        cls.$statics = static_attrs.length ? static_attrs : false;
        return cls;
    };

    var sx_Type = sx.Type = make_universal_constructor(null);
    sx.Type.construct = new_class;
    new_class.$cls = sx.Type;
    new_class.$name = 'construct';
    sx.Type.$statics = false;
    sx.Type.$own = false;

    var object_constructor = function() {};
    var sx_Object = sx.Object = make_universal_constructor(null);
    object_constructor.$name = 'construct';
    object_constructor.$cls = 'sx.Object';
    sx.Object.prototype = {
        construct: object_constructor,
        toString: function() {
            return '<instance of ' + this.$cls.$name + '>';
        }
    };
    sx.Object.construct = function() {
        var instance = instances.pop();
        instance.$cls = this;
        return instance;
    };
    sx.Object.construct.$cls = sx.Object;
    sx.Object.construct.$name = 'construct';


    sx.Object.$cls = sx.Type;
    sx.Object.$statics = false;
    sx.Object.$bases = [sx.Object];
    sx.Object.$name = 'Object';
    sx.Object.$module = 'sx';
    sx.Object.$mro = [sx.Object];
    sx.Object.$own = false;
    sx.Object.toString = function() { return '<sx.Object>'; };

    sx.Type.$bases = [sx.Object];
    sx.Type.$cls = sx.Type;
    sx.Type.$name = 'Type';
    sx.Object.$module = 'sx';
    sx.Type.$mro = [sx.Type, sx.Object];
    sx.Type.prototype = {
        construct: function() {}
    }
    sx.Type.prototype.construct.$cls = sx.Type;
    sx.Type.prototype.construct.$name = 'construct';

    sx.Type.toString = function() { return '<sx.Type>'; };

    sx.parent = parent;

    sx.define = function sx_define(name, bases, body) {
        var bases_len, metaclass, ms, i, ms_len, j, mcls, found, args;

        body = body || {};
        bases = bases || [];

        if (!name || typeof name != 'string') {
            throw new Error('Empty class name');
        }

        if (tos.call(bases) != '[object Array]') {
            throw new Error('Invalid `bases` for class "' + name + '"');
        }

        if (!body || typeof body != 'object') {
            throw new Error('Invalid `body` for class "' + name + '"');
        }

        bases_len = bases.length;
        if (bases_len == 0) {
            bases = [sx_Object];
        }

        metaclass = hop.call(body, 'metaclass') ? body.metaclass : null;
        if (metaclass == null) {
            if (!bases_len) {
                metaclass = sx_Type;
            } else if (bases_len == 1) {
                metaclass = bases[0].$cls;
            } else {
                ms = calc_metaclasses(bases);
                ms_len = ms.length;

                if (ms_len == 1) {
                    metaclass = ms[0];
                } else {
                    for (i = 0; i < ms_len; ++i) {
                        mcls = ms[i];
                        found = true;
                        for (j = 0; j < ms_len; ++j) {
                            if (!issubclass(mcls, ms[j])) {
                                found = false;
                                break;
                            }
                        }
                        if (found) {
                            metaclass = mcls;
                            break;
                        }
                    }

                    if (metaclass == null) {
                        throw new Error(error_mcls_conflict);
                    }
                }
            }
        } else if (issubclass(metaclass, sx_Type)) {
            ms = calc_metaclasses(bases);
            i = ms.length;

            for (; i--;) {
                if (!issubclass(metaclass, ms[i])) {
                    throw new Error(error_mcls_conflict);
                }
            }
        }

        if (arguments.length > 3) {
            args = [name, bases, body];
            args.push.apply(args, slice.call(arguments, 3));
            return metaclass.apply(null, args);
        } else {
            if (hop.call(metaclass, '$mro')) {
                return new metaclass(name, bases, body);
            } else {
                return metaclass(name, bases, body);
            }
        }
    };

    var issubclass = sx.issubclass = function sx_issubclass(cls, parents) {
        if (!cls || !hop.call(cls, '$mro')) {
            return false;
        }

        if (cls === parents) {
            return true;
        }

        var mro = cls.$mro;

        if (tos.call(parents) == '[object Array]') {
            var i = 0, len = parents.length;
            for (; i < len; ++i) {
                if (indexOf.call(mro, parents[i]) >= 0) {
                   return true;
                }
            }

            return false;

        } else if (hop.call(parents, '$mro')) {
            return indexOf.call(mro, parents) >= 0;
        }

        return false;
    };

    sx.isinstance = function sx_isinstance(inst, clss) {
        return issubclass(inst.$cls, clss);
    };
})();
