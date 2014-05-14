/*
* Copyright (c) 2012, 2014 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from . import sx_base


(function(global) {'use strict'; if (!sx.$bootstrap_class_system) {

sx.$bootstrap_class_system = function(opts) {
    'use strict';

    var StdArray = [].constructor,
        StdObject = {}.constructor,
        hop = StdObject.prototype.hasOwnProperty,
        tos = StdObject.prototype.toString,
        slice = StdArray.prototype.slice,
        indexOf = StdArray.prototype.indexOf,
        error_mcls_conflict = 'metaclass conflict: the metaclass of a derived class must be ' +
                              'a (non-strict) subclass of the metaclasses of all its bases',

        natives = [Array, Number, Date, Boolean, String, Object, RegExp],
        i,

        default_opts = {
            constructor_name: 'constructor',

            cls_attr: '$cls',
            mro_attr: '$mro',
            bases_attr: '$bases',
            name_attr: '$name',
            wrapped_attr: '$wrapped',
            own_attr: '$$own',
            statics_attr: '$$statics',
            module_attr: '$module',
            qualname_attr: '$qualname',
            last_instance_attr: '$$_lastinst',
            cached_constructor: '$$_cached_c',
            type_cls_name: 'type',
            object_cls_name: 'object',
            builtins_name: 'builtins',

            auto_register_ns: true
        };

    opts = opts || {};
    for (i in default_opts) {
        if (!hop.call(opts, i)) {
            opts[i] = default_opts[i];
        }
    }

    var CONSTRUCTOR = opts.constructor_name,
        CLS_ATTR = opts.cls_attr,
        MRO_ATTR = opts.mro_attr,
        BASES_ATTR = opts.bases_attr,
        NAME_ATTR = opts.name_attr,
        WRAPPED_ATTR = opts.wrapped_attr,
        OWN_ATTR = opts.own_attr,
        STATICS_ATTR = opts.statics_attr,
        MODULE_ATTR = opts.module_attr,
        QUALNAME_ATTR = opts.qualname_attr,
        AUTO_REGISTER_NS = opts.auto_register_ns,
        LAST_INST_ATTR = opts.last_instance_attr,
        ARGS_MARKER = {},
        CACHED_CONSTR = opts.cached_constructor,
        sx_ns_resolve, sx_ns;

    if (AUTO_REGISTER_NS) {
        if (sx.ns) {
            sx_ns_resolve = sx.ns.resolve;
            sx_ns = sx.ns;
        } else {
            sx_ns_resolve = sx_ns = function() {
                throw 'sx.ns is undefined';
            };
        }
    }

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
                s = seqs[i];
                if (s.length) {
                    nonemptyseqs.push(s);
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
        if (hop.call(cls, MRO_ATTR)) {
            return cls[MRO_ATTR].slice();
        }

        var lst = [[cls]],
            i = 0,
            bases = cls[BASES_ATTR],
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
            return [bases[0][CLS_ATTR]];
        }

        result = [];
        for (i = 0; i < bases_len; ++i) {
            mcls = bases[i][CLS_ATTR];

            if (indexOf.call(result, mcls) < 0) {
                result.push(mcls);
            }
        }
        return result;
    };

    function sx_find_parent(cls, ths, method) {
        var mro, mro_len, i, pos, base, func;

        if (hop.call(ths, MRO_ATTR) && !sx_issubclass(ths[CLS_ATTR], cls)) {
            // class

            mro = ths[MRO_ATTR];
            pos = indexOf.call(mro, cls);

            if (pos >= 0) {
                for (i = pos + 1; base = mro[i]; ++i) {
                    if (hop.call(base, method)) {
                        func = base[method];
                        if (hop.call(func, WRAPPED_ATTR)) {
                            func = func[WRAPPED_ATTR];
                        }
                        return func;
                    }
                }
            }
        } else {
            mro = ths[CLS_ATTR][MRO_ATTR];
            pos = indexOf.call(mro, cls);

            if (pos >= 0) {
                mro_len = mro.length;
                for (i = pos + 1; i < mro_len; ++i) {
                    base = mro[i].prototype;

                    if (hop.call(base, method)) {
                        return base[method];
                    }
                }
            }
        }

        throw new Error("can't find '" + method + "' parent method for '" + cls + "'");
    };

    function sx_parent(cls, ths, method, args) {
        if (AUTO_REGISTER_NS && typeof cls == 'string') {
            cls = sx_ns_resolve(cls);
        }

        args = args || [];
        if (arguments.length > 4) {
            throw new Error('invalid `parent` call, should receive arguments as array');
        }

        return sx_find_parent(cls, ths, method).apply(ths, args);
    };

    sx_parent.find = sx_find_parent;

    function make_universal_constructor() {
        // For compatibility with IE prior 9 version, we create a '_class' variable explicitly
        var _class = function _class_ctr(arg0, arg1 /*, ...*/) {
            var args, constr, ths = null, prev_instance;

            if (this instanceof _class) {
                args = arg0 === ARGS_MARKER ? arg1 : arguments;

                constr = _class[CACHED_CONSTR];
                if (constr === ObjectClass_Constructor) {
                    // Fast path for all classes without a custom
                    // static constructor
                    ths = this;
                    ths[CLS_ATTR] = _class;
                } else {
                    prev_instance = _class[LAST_INST_ATTR];
                    _class[LAST_INST_ATTR] = this;
                    ths = constr.apply(_class, args);
                    _class[LAST_INST_ATTR] = prev_instance;
                }

                if (ths == null) {
                    throw new Error(_class + ': could not create an instance');
                }

                if (hop.call(ths, CLS_ATTR)) {
                    constr = ths[CLS_ATTR].prototype[CACHED_CONSTR];
                    constr && constr.apply(ths, args);
                }

                return ths;
            } else {
                return new _class(ARGS_MARKER, arguments);
            }
        };
        return _class;
    };

    var boundmethod;
    if (Function.prototype.bind) {
        boundmethod = function(func, scope) {
            var bound = func.bind(scope);
            bound[WRAPPED_ATTR] = func;
            return bound;
        };
    } else {
        boundmethod = function(func, scope) {
            var bound = function() {
                return func.apply(scope, arguments);
            };
            bound[WRAPPED_ATTR] = func;
            return bound;
        };
    }

    function is_method(obj) {
        return (typeof obj == 'function') && !hop.call(obj, MRO_ATTR)
                                            && indexOf.call(natives, obj) == -1;
    };

    function fix_static_method(attr, static_name, cls) {
        if (!hop.call(attr, NAME_ATTR)) {
            attr[NAME_ATTR] = static_name;
        }

        if (hop.call(attr, WRAPPED_ATTR)) {
            attr = boundmethod(attr[WRAPPED_ATTR], cls);
        } else {
            attr = boundmethod(attr, cls);
        }

        attr[NAME_ATTR] = static_name;

        return attr;
    };

    function new_class(name, bases, dct) {
        var i, j, cls, parent, proto, attr,
            mro, attrs_flag = 0, parent_proto, mro_len, mro_len_1,
            static_attrs_cache = {}, static_attrs = [],
            statics, static_name, own = [], parent_own, parent_mro,
            keys, key, len;

        cls = make_universal_constructor();

        if (AUTO_REGISTER_NS && name.indexOf('.') != -1) {
            sx_ns(name, cls);
        }

        cls.toString = function() { return '<class ' + name + '>'; };

        if ((i = name.lastIndexOf('.')) !== -1) {
            cls[NAME_ATTR] = name.substr(i + 1);
            cls[MODULE_ATTR] = name.substr(0, i);
        } else {
            cls[NAME_ATTR] = name;
            cls[MODULE_ATTR] = null;
        }
        cls[QUALNAME_ATTR] = name;

        cls[CLS_ATTR] = this;
        proto = cls.prototype = {};

        cls[BASES_ATTR] = bases;
        mro = cls[MRO_ATTR] = calc_mro(cls);
        mro_len = mro.length;
        mro_len_1 = mro_len - 1;


        var constr = hop.call(dct, CONSTRUCTOR) && dct[CONSTRUCTOR];
        if (!constr) {
            for (i = 1; i < mro_len; i++) {
                parent = mro[i].prototype;
                if (hop.call(parent, CONSTRUCTOR)) {
                    constr = parent[CONSTRUCTOR];
                    break;
                }
            }
        }

        proto[CACHED_CONSTR] = constr; // false is OK too

        keys = sx.keys(dct);
        for (i = 0, len = keys.length; i < len; i++) {
            key = keys[i];
            if (attr != 'metaclass' && attr != 'statics') {
                attr = dct[key];
                if (attr != null && !hop.call(attr, NAME_ATTR) && is_method(attr)) {
                    attr[NAME_ATTR] = key;
                }
                proto[key] = attr;
                own.push(key);
            }
        }

        cls[OWN_ATTR] = (attrs_flag = own.length) ? own : false;

        for (i = 1; i < mro_len_1; ++i) {
            j = mro[i];
            parent_proto = j.prototype;
            parent_own = j[OWN_ATTR];

            if (parent_own) {
                for (j = parent_own.length; j--;) {
                    attr = parent_own[j];
                    if (!hop.call(proto, attr) && attr != CONSTRUCTOR) {
                        proto[attr] = parent_proto[attr];
                        (attr != CONSTRUCTOR) && (attrs_flag = 1);
                    }
                }
            }
        }

        if (!hop.call(proto, 'toString')) {
            proto.toString = ObjectClass.prototype.toString;
        }

        attrs_flag && (cls.$SX_CLS_ATTR$ = 1);

        constr = false;
        if (hop.call(dct, 'statics')) {
            statics = dct.statics;
            keys = sx.keys(statics);

            constr = hop.call(statics, CONSTRUCTOR) && statics[CONSTRUCTOR];

            for (i = 0, len = keys.length; i < len; i++) {
                static_name = keys[i];
                static_attrs_cache[static_name] = true;
                static_attrs.push(static_name);
                attr = statics[static_name];

                if (is_method(attr)) {
                    fix_static_method(attr, static_name, cls);
                }

                cls[static_name] = attr;
            }
        }

        if (!constr) {
            for (i = 1; i < mro_len; i++) {
                parent = mro[i];
                if (hop.call(parent, CONSTRUCTOR)) {
                    constr = parent[CONSTRUCTOR];
                    break;
                }
            }
        }
        cls[CACHED_CONSTR] = constr;


        for (i = 1; i < mro_len_1; ++i) {
            parent_mro = mro[i];
            statics = parent_mro[STATICS_ATTR];
            if (statics) {
                for (j = statics.length; j--;) {
                    static_name = statics[j];
                    if (!hop.call(static_attrs_cache, static_name) && static_name != CONSTRUCTOR) {
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

        if (hop.call(this, '$SX_CLS_ATTR$')) {
            parent_proto = this.prototype;
            for (static_name in parent_proto) {
                if (hop.call(parent_proto, static_name) && static_name != CONSTRUCTOR
                                                            && !hop.call(cls, static_name)) {
                    attr = parent_proto[static_name];

                    if (is_method(attr)) {
                        fix_static_method(attr, static_name, cls);
                    }

                    cls[static_name] = attr;
                }
            }
        }

        cls[STATICS_ATTR] = static_attrs.length ? static_attrs : false;
        cls[LAST_INST_ATTR] = null;
        return cls;
    };

    var TypeClass = make_universal_constructor();
    TypeClass[CACHED_CONSTR] = TypeClass[CONSTRUCTOR] = new_class;
    new_class[CLS_ATTR] = TypeClass;
    new_class[NAME_ATTR] = CONSTRUCTOR;
    TypeClass[STATICS_ATTR] = false;
    TypeClass[OWN_ATTR] = false;
    var type_qualname = TypeClass[QUALNAME_ATTR] = opts.type_cls_name;

    var object_constructor = function() {};
    var ObjectClass = make_universal_constructor();
    object_constructor[NAME_ATTR] = CONSTRUCTOR;
    object_constructor[CLS_ATTR] = ObjectClass;
    var ObjectClass_toString = function() {
        return '<instance of ' + this[CLS_ATTR][QUALNAME_ATTR] + '>';
    };
    ObjectClass_toString.$cls = ObjectClass;
    ObjectClass_toString.$name = 'toString';
    ObjectClass.prototype = {
        toString: ObjectClass_toString
    };
    ObjectClass.prototype[CONSTRUCTOR] = object_constructor;
    ObjectClass.prototype[CACHED_CONSTR] = false;
    var ObjectClass_Constructor = ObjectClass[CACHED_CONSTR] = ObjectClass[CONSTRUCTOR] = function() {
        var instance = this[LAST_INST_ATTR];
        instance[CLS_ATTR] = this;
        return instance;
    };
    ObjectClass[CONSTRUCTOR][CLS_ATTR] = ObjectClass;
    ObjectClass[CONSTRUCTOR][NAME_ATTR] = CONSTRUCTOR;


    ObjectClass[CLS_ATTR] = TypeClass;
    ObjectClass[STATICS_ATTR] = false;
    ObjectClass[BASES_ATTR] = [ObjectClass];
    ObjectClass[NAME_ATTR] = opts.object_cls_name;
    ObjectClass[MODULE_ATTR] = opts.builtins_name;
    var obj_qualname = ObjectClass[QUALNAME_ATTR] = opts.object_cls_name;
    ObjectClass[MRO_ATTR] = [ObjectClass];
    ObjectClass[OWN_ATTR] = false;
    ObjectClass.toString = function() { return '<class ' + obj_qualname + '>'; };

    TypeClass[BASES_ATTR] = [ObjectClass];
    TypeClass[CLS_ATTR] = TypeClass;
    TypeClass[NAME_ATTR] = opts.type_cls_name;
    TypeClass[MODULE_ATTR] = opts.builtins_name;
    TypeClass[MRO_ATTR] = [TypeClass, ObjectClass];
    TypeClass.prototype = {}
    TypeClass.prototype[CONSTRUCTOR] = function() {};
    TypeClass.prototype[CACHED_CONSTR] = false;
    TypeClass.prototype[CONSTRUCTOR][CLS_ATTR] = TypeClass;
    TypeClass.prototype[CONSTRUCTOR][NAME_ATTR] = CONSTRUCTOR;

    TypeClass.toString = function() { return '<class ' + type_qualname + '>'; };

    function find_metaclass(bases) {
        var bases_len = bases.length,
            ms, ms_len, i, mcls, j, found;

        if (bases_len == 1) {
            return bases[0][CLS_ATTR];
        } else {
            ms = calc_metaclasses(bases);
            ms_len = ms.length;

            if (ms_len == 1) {
                return ms[0];
            } else {
                for (i = 0; i < ms_len; ++i) {
                    mcls = ms[i];
                    found = true;
                    for (j = 0; j < ms_len; ++j) {
                        if (!sx_issubclass(mcls, ms[j])) {
                            found = false;
                            break;
                        }
                    }
                    if (found) {
                        return mcls;
                    }
                }
            }
        }

        throw new Error(error_mcls_conflict);
    }

    function validate_metaclass(bases, metaclass) {
        var ms, i;

        if (sx_issubclass(metaclass, TypeClass)) {
            ms = calc_metaclasses(bases);
            i = ms.length;

            for (; i--;) {
                if (!sx_issubclass(metaclass, ms[i])) {
                    throw new Error(error_mcls_conflict);
                }
            }
        }
    };

    function sx_define(name, bases, body) {
        var bases_len, metaclass, ms, i, args;

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
            bases.push(ObjectClass);
        }

        metaclass = hop.call(body, 'metaclass') ? body.metaclass : null;

        if (AUTO_REGISTER_NS) {
            for (i = 0; i < bases_len; i++) {
                if (typeof bases[i] == 'string') {
                    bases[i] = sx_ns_resolve(bases[i]);
                }
            }
            if (typeof metaclass == 'string') {
                metaclass = sx_ns_resolve(metaclass);
            }
        }

        if (metaclass == null) {
            metaclass = find_metaclass(bases);
        } else {
            validate_metaclass(bases, metaclass);
        }

        if (arguments.length > 3) {
            args = [name, bases, body];
            args.push.apply(args, slice.call(arguments, 3));
            return metaclass.apply(null, args);
        } else {
            if (hop.call(metaclass, MRO_ATTR)) {
                return new metaclass(name, bases, body);
            } else {
                return metaclass(name, bases, body);
            }
        }
    };

    // Low-level function that should be used by compilers only
    function sx_define_ll(module, name, bases, body, metaclass) {
        var cls;

        if (bases.length == 0) {
            bases.push(ObjectClass);
        }

        if (metaclass == null) {
            metaclass = find_metaclass(bases);
        } else {
            validate_metaclass(bases, metaclass);
        }

        if (hop.call(metaclass, MRO_ATTR)) {
            cls = new metaclass(name, bases, body);
        } else {
            cls = metaclass(name, bases, body);
        }

        cls[MODULE_ATTR] = module || null;

        return cls;
    };

    sx_define.new_class = sx_define_ll;

    // BaseObject is an empty class that is not intended
    // to be ever instantiated.  It's main purpose is
    // to play role of the 'root' javascript class, that
    // everything else is an instance of.
    function BaseObject() {
        throw new Error('BaseObject cannot be instantiated');
    }
    BaseObject.toString = function() { return '<class BaseObject>' }

    function sx_issubclass(cls, parents) {
        if (!cls || !hop.call(cls, MRO_ATTR)) {
            return false;
        }

        if (cls === parents) {
            return true;
        }

        var mro = cls[MRO_ATTR];

        if (tos.call(parents) == '[object Array]') {
            var i = 0, len = parents.length, p;
            for (; i < len; ++i) {
                p = parents[i];
                if (p === StdObject || indexOf.call(mro, p) >= 0) {
                   return true;
                }
            }

            return false;

        } else if (hop.call(parents, MRO_ATTR)) {
            return indexOf.call(mro, parents) >= 0 || parents === StdObject;
        }

        return false;
    };

    function _instanceof(inst, cls) {
        if (inst instanceof cls) {
            return true;
        }

        if (cls === BaseObject) {
            return true;
        }

        if (cls === String && typeof inst == 'string') {
            return true;
        }

        if (cls === Number && typeof inst == 'number') {
            return true;
        }

        if (cls === Boolean && typeof inst == 'boolean') {
            return true;
        }

        return false;
    };

    function sx_isinstance(inst, clss) {
        var i, len;

        if (inst == null) {
            return false;
        }

        if (!hop.call(inst, CLS_ATTR)) {
            if (tos.call(clss) == '[object Array]') {
                for (i = 0, len = clss.length; i < len; i++) {
                    if (_instanceof(inst, clss[i])) {
                        return true;
                    }
                }
                return false;
            } else {
                return _instanceof(inst, clss);
            }
        }

        return sx_issubclass(inst[CLS_ATTR], clss);
    };

    return {
        BaseObject: BaseObject,
        object: ObjectClass,
        type: TypeClass,
        define: sx_define,
        issubclass: sx_issubclass,
        isinstance: sx_isinstance,
        parent: sx_parent
    }
};


sx.apply(sx, sx.$bootstrap_class_system());

}})(this);
