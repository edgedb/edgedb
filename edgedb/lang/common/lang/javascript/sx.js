/*
* Copyright (c) 2011 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


this.sx = (function() {
    var sx = function(selector) { return new sx._fn.init(selector); },
        _is_id = /^#([\w\-]*)$/,
        _id_counter = 0;

    function _extend(obj, ex) {
        for (var i in ex) {
            if (ex.hasOwnProperty(i)) {
                obj[i] = ex[i];
            }
        }
        return obj;
    };

    _extend(sx, {
        is_array: function(obj) {
            return Object.prototype.toString.call(obj) === '[object Array]';
        },

        is_function: function(obj) {
            return Object.prototype.toString.call(obj) === '[object Function]';
        },

        is_object: function(obj) {
            return !!obj && Object.prototype.toString.call(obj) === '[object Object]';
        },

        _validate_sx_object: function(obj) {
            return Object.prototype.hasOwnProperty.call(obj, '_secret_')
                                                    && obj._secret_ === sx._secret_;
        },

        is_class: function(obj) {
            return sx._validate_sx_object(obj) && sx.is_function(obj)
                                            && sx.getattr(obj, '_type_', null) === 'class';
        },

        eq: function(obj1, obj2) {
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

        each: function(obj, func, scope) {
            scope = scope || window;

            if (func.length < 1 || func.length > 2) {
                throw 'invalid each function "' + func + '", should have one or two arguments only';
            }

            var i, r, no_desc = (func.length <= 2);

            if (sx.is_array(obj) || obj instanceof sx) {
                for (i = 0; i < obj.length; i++) {
                    if (no_desc) {
                        r = func.call(scope, obj[i], i);
                    } else {
                        r = func.call(scope, obj[i], i, {first: i == 0, last: i == obj.length - 1, key: i});
                    }

                    if (typeof r != 'undefined') {
                        return r;
                    }
                }
            } else if (sx.is_object(obj)) {
                var cnt = 0, len = sx.len(obj);
                for (i in obj) {
                    if (obj.hasOwnProperty(i)) {
                        if (no_desc) {
                            r = func.call(scope, obj[i], i);
                        } else {
                            r = func.call(scope, obj[i], i, {first: cnt == 0, last: cnt == len - 1, key: i});
                        }

                        if (typeof r != 'undefined') {
                            return r;
                        }
                        cnt++;
                    }
                }
            } else {
                throw "unable to iterate non-iterable object '" + obj +
                      "', has to be eother array or object";
            }
        },

        keys: function(obj) {
            if (sx.is_object(obj)) {
                var result = []
                for (var k in obj) {
                    if (obj.hasOwnProperty(k)) {
                        result.push(k);
                    }
                }

                return result;
            } else {
                throw 'sx.keys function only supports objects, got ' + (typeof obj);
            }
        },

        len: function(obj) {
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

            throw 'sx.len function supports only objects and arrays, got ' + (typeof obj);
        },

        getattr: function(obj, attr, def) {
            if (obj.hasOwnProperty(attr)) {
                return obj[attr];
            } else {
                if (typeof def == 'undefined') {
                    throw (typeof obj) + ': unable to get attribute ' + attr;
                }
                return def;
            }
        },

        contains: function(container, obj) {
            if (sx.is_object(container)) {
                return container.hasOwnProperty(obj);
            }

            if (sx.is_array(container)) {
                if (Array.prototype.hasOwnProperty('indexOf')) {
                    return container.indexOf(obj) != -1;
                } else {
                    for (var i = 0; i < container.length; i++) {
                        if (container[i] == obj) {
                            return true;
                        }
                    }

                    return false;
                }
            }

            throw 'sx.contains function supports only objects and arrays, got ' + (typeof obj);
        },

        partial: function(func, scope/*, arg0, arg2, ...*/) {
            var args = null;

            if (arguments.length > 2) {
                args = Array.prototype.slice.call(arguments, 2);
            }

            if (typeof scope == 'undefined') {
                scope = window;
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
        },

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
            }
        },

        dom: {
            id: function(suffix) {
                return 'sx-id-' + (++_id_counter) + (suffix || '');
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
                if (typeof scope == 'undefined') {
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
                    new_el = document.createTextNode(sx.escape(text));
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
                            throw "non-DOM element passed in array to sx() function: " + selector[i];
                        }
                    }

                    this.length = selector.length;
                }

                return this;
            }

            if (typeof selector != 'string') {
                throw "non-string selector passed to 'sx' function: " + selector;
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
