/*
* Copyright (c) 2011-2012 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


this.sx = (function() {
    'use strict';

    var undefined = void(0),
        sx = function(selector) { return new sx._fn.init(selector); },
        _is_id = /^#([\w\-]*)$/,
        _id_counter = 0,
        global = (typeof window == 'undefined' ? this : window),
        has_own_property = Object.prototype.hasOwnProperty;

    function _extend(obj, ex) {
        var i;
        for (i in ex) {
            if (has_own_property.call(ex, i)) {
                obj[i] = ex[i];
            }
        }
        return obj;
    };

    var Error = function(msg) {
        this.message = msg;
    };
    Error.prototype = {
        name: 'sx.Error',
        toString: function() {
            return this.name + ': ' + this.message;
        }
    };
    Error.toString = function() {
        return 'sx.Error';
    };

    _extend(sx, {
        Error: Error,

        id: function(suffix) {
            return 'sx-id-' + (++_id_counter) + (suffix || '');
        },

        apply: _extend,

        is_array: function sx_is_array(obj) {
            return Object.prototype.toString.call(obj) === '[object Array]';
        },

        is_function: function sx_is_function(obj) {
            return Object.prototype.toString.call(obj) === '[object Function]';
        },

        is_object: function sx_is_object(obj) {
            return !!obj && Object.prototype.toString.call(obj) === '[object Object]';
        },

        is_string: function sx_is_string(obj) {
            return Object.prototype.toString.call(obj) === '[object String]';
        },

        _validate_sx_object: function(obj) {
            return Object.prototype.hasOwnProperty.call(obj, '_secret_')
                                                    && obj._secret_ === sx._secret_;
        },

        eq: function sx_eq(obj1, obj2) {
            if (typeof obj1 != typeof obj2) {
                return false;
            }

            var i;

            if (sx.is_array(obj1) && sx.is_array(obj2)) {
                if (obj1.length != obj2.length) {
                    return false;
                }

                for (i = 0; i < obj1.length; i++) {
                    if (!sx.eq(obj1[i], obj2[i])) {
                        return false;
                    }
                }

                return true;
            }

            if (sx.is_object(obj1) && sx.is_object(obj2)) {
                if (sx.len(obj1) != sx.len(obj2)) {
                    return false;
                }

                for (i in obj1) {
                    if (!sx.eq(obj1[i], obj2[i])) {
                        return false;
                    }
                }

                return true;
            }

            return obj1 === obj2;
        },

        each: function sx_each(obj, func, scope) {
            scope = scope || global;

            if (func.length < 1 || func.length > 3) {
                throw new Error('invalid each function "' + func +
                                '", should have one, two or three arguments only');
            }

            var i, r, no_desc = (func.length <= 2), len;

            if (sx.is_array(obj) || obj instanceof sx) {
                len = obj.length;
                if (no_desc) {
                    for (i = 0; i < len; i++) {
                        r = func.call(scope, obj[i], i);

                        if (r !== undefined) {
                            return r;
                        }
                    }
                } else {
                    for (i = 0; i < len; i++) {
                        r = func.call(scope, obj[i], i, {
                            first: i == 0,
                            last: i == obj.length - 1,
                            key: i
                        });

                        if (r !== undefined) {
                            return r;
                        }
                    }
                }

            } else if (sx.is_object(obj)) {
                var cnt = 0;
                len = sx.len(obj);

                if (no_desc) {
                    for (i in obj) {
                        if (obj.hasOwnProperty(i)) {
                            r = func.call(scope, obj[i], i);

                            if (r !== undefined) {
                                return r;
                            }
                            cnt++;
                        }
                    }
                } else {
                    for (i in obj) {
                        if (obj.hasOwnProperty(i)) {
                            r = func.call(scope, obj[i], i, {
                                first: cnt == 0,
                                last: cnt == len - 1,
                                key: i
                            });

                            if (r !== undefined) {
                                return r;
                            }
                            cnt++;
                        }
                    }
                }

            } else {
                throw new Error("unable to iterate non-iterable object '" + obj +
                                "', has to be either array or object");
            }
        },

        map: function(obj, func, scope) {
            scope = scope || window;

            var result;
            var no_desc = (func.length <= 2);
            var map_func;

            if (sx.is_array(obj) || obj instanceof sx) {
                result = [];
            } else if (sx.is_object(obj)) {
                result = {};
            } else {
                throw new Error("unable to map on non-iterable object '" + obj +
                                "', has to be either array or object");
            }

            if (no_desc) {
                map_func = function(el, i) {
                    result[i] = func.call(scope, el, i);
                };
            } else {
                map_func = function(el, i, options) {
                    result[i] = func.call(scope, el, i, options);
                };
            }

            sx.each(obj, map_func, scope);

            return result;
        },

        filter: function(obj, func, scope) {
            scope = scope || window;

            var is_array = (sx.is_array(obj) || obj instanceof sx);
            var result;
            var no_desc = (func.length <= 2);
            var filter_func;

            if (is_array) {
                result = [];

                if (no_desc) {
                    filter_func = function(el, i) {
                        if (func.call(scope, el, i)) {
                            result.push(el);
                        }
                    };
                } else {
                    filter_func = function(el, i, options) {
                        if (func.call(scope, el, i, options)) {
                            result.push(el);
                        }
                    };
                }
            } else if (sx.is_object(obj)) {
                result = {};

                if (no_desc) {
                    filter_func = function(el, i) {
                        if (func.call(scope, el, i)) {
                            result[i] = el;
                        }
                    };
                } else {
                    filter_func = function(el, i, options) {
                        if (func.call(scope, el, i, options)) {
                            result[i] = el;
                        }
                    };
                }
            } else {
                throw new Error("unable to filter on non-iterable object '" + obj +
                                "', has to be either array or object");
            }

            sx.each(obj, filter_func, scope);

            return result;
        },

        keys: function sx_keys(obj) {
            if (sx.is_object(obj)) {
                var result = []
                for (var k in obj) {
                    if (has_own_property.call(obj, k)) {
                        result.push(k);
                    }
                }

                return result;
            } else {
                throw new Error('sx.keys function only supports objects, got ' + (typeof obj));
            }
        },

        len: function sx_len(obj) {
            if (sx.is_array(obj) || obj instanceof sx) {
                return obj.length;
            }

            if (sx.is_object(obj)) {
                var attr, len = 0;
                for (attr in obj) {
                    if (obj.hasOwnProperty(attr)) {
                        len++;
                    }
                }
                return len;
            }

            if (sx.is_string(obj)) {
                return obj.length;
            }

            throw new Error('sx.len function supports only objects and arrays, got ' +
                            (typeof obj));
        },

        hasattr: function sx_hasattr(obj, attr, weak) {
            if (obj === null || obj === undefined) {
                return false;
            }

            if (weak) {
                return obj[attr] !== undefined;
            } else {
                return has_own_property.call(obj, attr);
            }
        },

        first: function(obj, def) {
            var is_array = (sx.is_array(obj) || obj instanceof sx);
            if (is_array) {
                if (obj.length > 0) {
                    return obj[0];
                } else {
                    if (def !== undefined) {
                        return def;
                    } else {
                        throw new Error('sx.first: empty array passed with no default value set');
                    }
                }
            } else if (sx.is_object(obj)) {
                var i;
                for (i in obj) {
                    if (has_own_property.call(obj, i)) {
                        return obj[i];
                    }
                }

                if (def !== undefined) {
                    return def;
                }

                throw new Error('sx.first: empty object passed with no default value set');
            } else if (sx.is_string(obj)) {
                if (obj.length > 0) {
                    return obj[0];
                } else {
                    if (def !== undefined) {
                        return def;
                    } else {
                        throw new Error('sx.first: empty string passed with no default value set');
                    }
                }
            }

            throw new Error('sx.first supports only arrays and objects');
        },

        getattr: function sx_getattr(obj, attr, def) {
            if (has_own_property.call(obj, attr)) {
                return obj[attr];
            } else {
                if (def === undefined) {
                    throw new Error((typeof obj) + ': unable to get attribute ' + attr);
                }
                return def;
            }
        },

        contains: function(container, obj) {
            if (sx.is_object(container)) {
                if (!sx.is_string(obj)) {
                    throw new Error('sx.contains: only strings may be keys in javascript ' +
                                    'objects, got ' + (typeof obj));
                }
                return has_own_property.call(container, obj);
            }

            if (sx.is_array(container)) {
                if (Array.prototype.hasOwnProperty('indexOf')) {
                    return container.indexOf(obj) != -1;
                } else {
                    for (var i = 0; i < container.length; i++) {
                        if (container[i] === obj) {
                            return true;
                        }
                    }

                    return false;
                }
            }

            if (sx.is_string(container)) {
                if (!sx.is_string(obj)) {
                    throw new Error('sx.contains: expected string "obj" value, got ' +
                                     (typeof obj));
                }
                return container.indexOf(obj) !== -1;
            }

            throw new Error('sx.contains function supports only objects and arrays, got ' +
                            (typeof obj));
        },

        partial: (function() {
            if (Function.prototype.bind) {
                var func = Function.prototype.call.bind(Function.prototype.bind);
                func.displayName = 'sx_partial';
                return func;
            } else {
                return function sx_partial(func, scope/*, arg0, arg2, ...*/) {
                    var args = null;

                    if (arguments.length > 2) {
                        args = Array.prototype.slice.call(arguments, 2);
                    }

                    if (scope === undefined) {
                        scope = global;
                    }

                    if (args && args.length) {
                        return (function(func, scope, args) {
                            return function() {
                                var args_copy = Array.prototype.slice.call(args);
                                args_copy.push.apply(args_copy, arguments);
                                return func.apply(scope, args_copy);
                            };
                        })(func, scope, args);
                    } else {
                        return (function(func, scope) {
                            return function() {
                                return func.apply(scope, arguments);
                            };
                        })(func, scope);
                    }
                };
            }
        })(),

        escape: function(str) {
            return String(str).replace(/\&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
        },

        uuid4: function() {
            function s(num) {
                return (((1 + Math.random()) * num) | 0).toString(16);
            }

            return s(0x10000000) + '-' + s(0x1000) + '-4' + s(0x100) + '-a' +
                                                s(0x100) + '-' + s(0x1000) + s(0x1000) + s(0x1000);
        },

        str: {
            shorten: function(str, max) {
                if (str.length >= max) {
                    str = str.substring(0, max) + '...';
                }
                return str;
            },

            trim: function(str) {
                return str.replace(/^\s\s*/, '').replace(/\s\s*$/, '');
            },

            startswith: function(str, sub) {
                return str.indexOf(sub) === 0;
            },

            endswith: function(str, sub) {
                var sublen = sub.length, strlen = str.length;

                if (!sublen) {
                    return true;
                }

                if (sublen > strlen) {
                    return false;
                }

                return str.lastIndexOf(sub) === str.length - sublen;
            }
        },

        dom: {
            id: function(suffix) {
                // sx.dom.id() is deprecated. Use sx.id() instead.
                return sx.id(suffix);
            },

            has_class: function(element, class_name) {
                if (!class_name || !element) {
                    return false;
                }

                return (' ' + element.className + ' ').indexOf(' ' + class_name + ' ') !== -1;
            },

            add_class: function(element, class_name) {
                if (!element || !class_name || typeof class_name != 'string' ||
                        !element.nodeType || element.nodeType != 1) {
                    return;
                }

                if (element.classList && class_name.indexOf(' ') == -1) {
                    return element.classList.add(class_name);
                }

                if (element.className && element.className.length) {
                    if (!element.className.match('\\b' + class_name + '\\b')) {
                        element.className += ' ' + class_name;
                    }
                } else {
                    element.className = class_name;
                }
            },

            remove_class: function(element, class_name) {
                if (!element || !class_name || typeof class_name != 'string' ||
                        !element.nodeType || element.nodeType != 1) {
                    return;
                }

                if (element.classList) {
                    return element.classList.remove(class_name);
                }

                if (element.className) {
                    var reg = new RegExp('(\\s|^)' + cls + '(\\s|$)');
                    element.className = element.className.replace(reg, ' ');
                }
            },

            toggle_class: function(element, class_name) {
                if (!class_name || !element) {
                    return false;
                }

                if (sx.dom.has_class(element, class_name)) {
                    sx.dom.remove_class(element, class_name);
                } else {
                    sx.dom.add_class(element, class_name);
                }
            },

            on: function(element, event, callback, scope/*, arg0, arg1, ... */) {
                if (scope === undefined) {
                    scope = element;
                }

                var args = Array.prototype.slice.call(arguments, 4);
                args.splice(0, 0, callback, scope);
                callback = sx.partial.apply(this, args);

                if (element.addEventListener) {
                    element.addEventListener(event, callback, false);
                } else if (element.attachEvent) {
                    element.attachEvent('on' + event, callback);
                }
            },

            _builder: function(root, spec) {
                var tag = spec['tag'] || null,
                    cls = spec['cls'] || null,
                    html = spec['html'] || null,
                    text = spec['text'] || null,
                    id = spec['id'] || null,
                    children = spec['children'] || null,
                    attrs = spec['attrs'] || null,
                    new_el, i;

                if (text && root && !tag && !html && !children && !id && !cls && !attrs) {
                    new_el = document.createTextNode(text);
                } else {
                    new_el = document.createElement(tag || 'div');

                    if (cls) {
                        sx.dom.add_class(new_el, cls);
                    }

                    if (id) {
                        new_el.setAttribute('id', id);
                    }

                    if (children && sx.is_object(children) && !sx.is_array(children)) {
                        children = [children];
                    }

                    if (children && children.length) {
                        for (i = 0; i < children.length; i++) {
                            sx.dom._builder(new_el, children[i]);
                        }
                    } else if (text) {
                        new_el.innerHTML = sx.escape(text);
                    } else if (html) {
                        new_el.innerHTML = html;
                    }

                    if (attrs) {
                        for (i in attrs) {
                            if (attrs.hasOwnProperty(i)) {
                                new_el.setAttribute(i, attrs[i]);
                            }
                        }
                    }
                }

                if (root) {
                    root.appendChild(new_el);
                }

                return new_el;
            },

            gen_html: function(spec) {
                var el = sx.dom._builder(null, {children: spec});
                return el.innerHTML;
            },

            replace: function(element, spec) {
                var child = sx.dom._builder(null, spec);
                element.parentNode.replaceChild(child, element);
            },

            update: function(element, spec) {
                if (typeof spec == 'string') {
                    element.innerHTML = spec;
                } else {
                    element.innerHTML = '';
                    element.appendChild(sx.dom._builder(null, spec));
                }
            }
        }
    });

    sx._fn = sx.prototype = {
        constructor: sx,

        init: function(selector) {
            this.length = 0;

            if (!selector) {
                // In case of empty, null or undefined ``selector``
                //
                return this;
            }

            if (selector.nodeType) {
                // ``selector`` is a DOM element
                //
                this[0] = selector;
                this.length = 1;
                return this;
            }

            if (sx.is_array(selector)) {
                if (selector.length) {
                    // Assume we're dealing with an array of DOM elements
                    //
                    for (var i = 0; i < selector.length; i++) {
                        if (selector[i].nodeType) {
                            this[i] = selector[i];
                        } else {
                            throw new Error("non-DOM element passed in array to sx() function: " +
                                            selector[i]);
                        }
                    }

                    this.length = selector.length;
                }

                return this;
            }

            if (typeof selector != 'string') {
                throw new Error("non-string selector passed to 'sx' function: " + selector);
            }

            var match = _is_id.exec(selector);
            if (match && match.length && match[1]) {
                // selector matches '#(some_id)'
                //
                var el = document.getElementById(match[1]);

                if (el) {
                    this.length = 1;
                    this[0] = el;
                }
            } else {
                var els = document.querySelectorAll(selector);
                if (els && els.length) {
                    for (var i = 0; i < els.length; i++) {
                        this[i] = els[i];
                    }
                    this.length = els.length;
                }
            }

            return this;
        },

        each: function(func, scope) {
            sx.each(this, func, scope);
            return this;
        },

        empty: function() {
            return this.length <= 0;
        },

        has_class: function(class_name) {
            if (!this.length) {
                return false;
            }

            for (var i = 0; i < this.length; i++) {
                if (!sx.dom.has_class(this[i], class_name)) {
                    return false;
                }
            }

            return true;
        },

        add_class: function(class_name) {
            for (var i = 0; i < this.length; i++) {
                sx.dom.add_class(this[i], class_name);
            }
            return this;
        },

        remove_class: function(class_name) {
            for (var i = 0; i < this.length; i++) {
                sx.dom.remove_class(this[i], class_name);
            }
            return this;
        },

        toggle_class: function(class_name) {
            for (var i = 0; i < this.length; i++) {
                sx.dom.toggle_class(this[i], class_name);
            }
            return this;
        },

        update: function(spec) {
            for (var i = 0; i < this.length; i++) {
                sx.dom.update(this[i], spec);
            }
            return this;
        },

        on: function(event, callback, scope/*, arg0, arg2*/) {
            var args = Array.prototype.slice.call(arguments, 0);
            args.splice(0, 0, 'tmp');

            for (var i = 0; i < this.length; i++) {
                args.splice(0, 1, this[i]);
                sx.dom.on.apply(this, args);
            }
            return this;
        }
    };

    sx._fn.init.prototype = sx._fn;

    return sx;
})();
