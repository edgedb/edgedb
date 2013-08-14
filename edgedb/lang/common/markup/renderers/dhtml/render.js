/*
* Copyright (c) 2011-2013 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from metamagic.utils.lang.javascript import sx, ds, uuid


(function(global) { 'use strict'; if (!sx.Markup) {

var hop = Object.prototype.hasOwnProperty;

sx.Markup = sx.Markup || {};

sx.Markup.Renderer = function(markup) {
    this.ex_depth = 0;
    this.tree_depth = 0;
    this.section_depth = 0;

    this.objects = {};

    this.handlers = {};

    this._id_base = sx.UUID.uuid4();
    this._id = 0;

    this.markup = markup;
    this._render_ctx = null;

    this.top_exc_title = null;
}

sx.Markup.Renderer.prototype = {
    on_ref_click: function(ref_id, object_id) {
        sx.dom.replace(sx('#' + ref_id)[0], this._render(this.objects[object_id]));
        this._rebind_handlers();
    },

    on_collapsible_click: function(collapsible_id, event) {
        if (event.stopPropagation) {
            event.stopPropagation();
        }

        if (event.metaKey || event.ctrlKey) {
            var parent = sx('#' + collapsible_id);
            parent.toggle_class('collapsed');
            if (parent.has_class('collapsed')) {
                sx('#' + collapsible_id + ' .sx-collapsible').add_class('collapsed');
            } else {
                sx('#' + collapsible_id + ' .sx-collapsible').remove_class('collapsed');
            }
        } else {
            sx('#' + collapsible_id).toggle_class('collapsed');
        }
    },

    on_collapsible_toggler_click: function(id, event) {
        sx('#' + id).toggle_class('collapsed');

        if (event.stopPropagation) {
            event.stopPropagation();
        }
    },

    'doc.Text': function(o) {
        return {
            tag: 'span',
            cls: 'doc-text',
            text: o.text
        };
    },

    'doc.SourceCode': function(o) {
        return {
            tag: 'div',
            cls: 'doc-source-code',
            text: o.text
        };
    },

    'doc.Section': function(o) {
        this.section_depth++;

        var body = [], i, obj;

        for (i = 0; i < o.body.length; i++) {
            body.push(this._render(o.body[i]));
        }

        if (o.title) {
            obj = this._render_collapsible({
                cls: 'doc-section doc-level-' + this.section_depth,

                label: o.title,
                label_cls: 'doc-section-title',
                collapsed: o.collapsed || this.section_depth > 2,

                body: {tag: 'div', cls: 'doc-section-body', children: body}
            });
        } else {
            obj = {
                cls: 'doc-section doc-level-' + this.section_depth,
                children: [{tag: 'div', cls: 'doc-section-body', children: body}]
            };
        }

        this.section_depth--;
        return obj;
    },

    'lang.Ref': function(o) {
        var id = this._gen_id();

        this.handlers[id] = {
            click: sx.partial(this.on_ref_click, this, id, o.ref)
        };

        return {
            tag: 'span',
            cls: 'lng-ref',
            attrs: {
                id: id
            },
            text: o.refname + ' <' + this._render_id(o.ref) + '>'
        }
    },

    'lang.NoneConstantType': function(o) {
        return {
            tag: 'span',
            cls: 'lng-constant',
            text: 'None'
        };
    },

    'lang.TrueConstantType': function(o) {
        return {
            tag: 'span',
            cls: 'lng-constant',
            text: 'True'
        };
    },

    'lang.FalseConstantType': function(o) {
        return {
            tag: 'span',
            cls: 'lng-constant',
            text: 'False'
        };
    },

    'lang.String': function(o) {
        return {
            tag: 'span',
            cls: 'lng-str',
            text: '"' + o.str + '"'
        };
    },

    'lang.Object': function(o) {
        var id = '';
        if (o.id) {
            id = ' at: ' + this._render_id(o.id);
        }

        var obj_name = o.class_module + '.' + o.class_name + id,
            children = [];

        if (o.attributes) {
            children.push({tag: 'span', cls: 'lng-obj-name', text: '<' + obj_name});

            if (o.attributes) {
                children.push(this._render_mapping(o.attributes));
            }

            children.push({tag: 'span', cls: 'lng-obj-name', text: '>'});
        } else {
            if (o.repr) {
                children.push({tag: 'span', cls: 'lng-obj-name', text: o.repr})
            } else {
                children.push({tag: 'span', cls: 'lng-obj-name', text: '<' + obj_name + '>'})
            }
        }

        return {
            tag: 'div',
            cls: 'lng-object',
            children: children
        };
    },

    'lang.Number': function(o) {
        return {
            tag: 'span',
            cls: 'lng-number',
            text: o.num
        };
    },

    'lang.List': function(o) {
        var items = [];

        items.push({tag: 'span', cls: 'sx-markup-bracket', text: '[ '});

        if (o.items) {
            for (var i = 0; i < o.items.length; i++) {
                var cls = 'lng-list-item';

                if (i == o.items.length - 1) {
                    cls += ' last';
                }

                var item = this._render(o.items[i]);

                if (item.cls) {
                    item.cls += ' ' + cls;
                } else {
                    item.cls = cls;
                }

                items.push(item);

                if (i != o.items.length - 1) {
                    items.push({text: ', '});
                }
            }

            if (o.trimmed) {
                items.push({text: '...'});
            }
        }

        items.push({tag: 'span', cls: 'sx-markup-bracket', text: ' ]'});

        var obj = {
            tag: 'div',
            cls: 'lng-list',
            children: items
        };

        return obj;
    },

    _render_mapping: function(map, trimmed) {
        var items = [];
        trimmed = trimmed || false;

        if (map) {
            for (var i in map) {
                if (map.hasOwnProperty(i)) {
                    var li = [{tag: 'div', cls: 'lng-dict-key', text: i}]
                    li.push(this._render(map[i]));

                    items.push({
                        tag: 'li',
                        children: li
                    });
                }
            }
        }

        if (trimmed) {
            items.push({tag: 'li', text: '...'});
        }

        var obj = {
            tag: 'div',
            cls: 'lng-dict',
            children: [
                {tag: 'div', cls: 'sx-markup-bracket', text: '{ '},
                {tag: 'ul', children: items},
                {tag: 'div', cls: 'sx-markup-bracket', text: ' }'}
            ]
        };

        return obj;
    },

    'lang.Dict': function(o) {
        return this._render_mapping(o.items, o.trimmed);
    },

    'lang.TreeNodeChild': function(o) {
        var body = [];

        if (o.label) {
            body.push({tag: 'div', cls: 'lng-tree-child-title', text: o.label});
        }

        body.push({tag: 'div', cls: 'lng-tree-node-child-body',
            children: this._render(o.node)
        });

        return {
            tag: 'li',
            children: body
        };
    },

    'lang.TreeNode': function(o) {
        this.tree_depth++;
        var children = [], i;

        if (o.children != null) {
            for (i = 0; i < o.children.length; i++) {
                children.push(this._render(o.children[i]));
            }
        }

        var label = o.name;
        if (o.id) {
            label = {tag: 'span', children: [
                         {text: label},
                         {tag: 'span', cls: 'lng-tree-node-id', text: this._render_id(o.id)}
                     ]};
        }

        var obj = this._render_collapsible({
            cls: 'lng-tree' + (this.tree_depth == 1 ? ' lng-tree-root' : ''),

            label: label,
            label_cls: 'lng-tree-title',
            collapsed: this.tree_depth > 1,

            body: {tag: 'ul', children: children}
        });

        this.tree_depth--;
        return obj;
    },

    'lang.ExceptionContext': function(o) {
        var body = [];

        if (o.body) {
            for (var i = 0; i < o.body.length; i++) {
                body.push(this._render(o.body[i]));
            }
        }

        var obj = {
            cls: 'exc-context',
            children: [
                {tag: 'h3', cls: 'exc-context-title', text: o.title},
                {cls: 'exc-context-body', children: body}
            ]
        };

        return obj;
    },

    'lang.Exception': function(o, obj) {
        this.ex_depth++;

        var cause = [];

        var body = [], msg = o.msg, msg_el,
            cls_name = o.class_module + '.' + o.class_name + ': ';

        if (o.cause) {
            cause.push(this._render(o.cause));
            cause.push(this._render_hr('The above exception was the direct ' +
                                                  'cause of the following exception'));
        } else if (o.context) {
            cause.push(this._render(o.context));
            cause.push(this._render_hr('During handling of the above exception, ' +
                                                        'another exception occurred'));
        }

        if (msg.length > 200) {
            msg_el = this._render_long_string({
                text: msg,
                maxlen: 200,
                detect_code: true
            });
        } else {
            msg_el = {text: msg};
        }

        body.push({tag: 'h2', cls: 'exc-title', children: [
                     {tag: 'span', cls: 'exc-num', children: [
                          {tag: 'span', text: '#'},
                          {text: this.ex_depth + ' '}
                      ]},

                     {tag: 'span', cls: 'exc-class',
                      text: cls_name},

                     {tag: 'span', cls: 'exc-msg', children: msg_el}
                  ]});

        if (o.contexts) {
            for (var i = 0; i < o.contexts.length; i++) {
                body.push(this._render(o.contexts[i]));
            }
        }

        var obj = {
            cls: 'sx-exception',
            children: body
        };

        cause.push(obj);
        this.ex_depth--;

        if (this.ex_depth == 0 &&
                (!this._render_ctx.hasOwnProperty('top_exception_header') ||
                        this._render_ctx.top_exception_header)) {

            this.top_exc_title = sx.str.shorten(msg, 50);

            var header = {tag: 'div', cls: 'exc-header', children: {
                            tag: 'h1',
                            children: [
                               {tag: 'span', cls: 'exc-class',
                                text: cls_name},

                               {tag: 'span', cls: 'exc-msg', text: sx.str.shorten(msg, 120)}
                            ]
                         }};

            cause.splice(0, 0, header);
        }

        obj = {
            cls: 'sx-exceptions',
            children: cause
        }

        return obj;
    },

    'lang.TracebackPoint': function(o) {
        var source = [],
            id = this._gen_id();

        this.handlers[id] = {
            click: sx.partial(this.on_collapsible_toggler_click, this, id)
        };

        if (o.lines) {
            for (var i = 0; i < o.lines.length; i++) {
                var lineno = o.line_numbers[i], line = o.lines[i], current = lineno == o.lineno,
                    cls = 'tb-line-line',
                    children = [];

                if (current) {
                    cls += ' tb-current';
                }

                children.push({tag: 'span', cls: 'tb-lineno', text: lineno});
                children.push({tag: 'span', cls: 'tb-code', text: line});

                if (current && o.colno != null) {
                    var _indent = '';
                    for (var col = 1; col < o.colno; col += 1) {
                        _indent += ' ';
                    }

                    children.push({tag: 'span', cls: 'tb-lineno sx-invisible', text: lineno});
                    children.push({
                        tag: 'div',
                        cls: 'tb-source-caret-line',
                        children: [
                            {tag: 'span', cls: 'tb-lineno sx-invisible', text: lineno},
                            {tag: 'span', cls: 'tb-source-caret', text: _indent + '^'}
                        ]
                    });
                }

                source.push({cls: cls, children: children});
            }
        }

        var tb_lines = {
            cls: 'tb-line',
            children: [
                {cls: 'tb-line-header', children: [
                     {text: 'File '},
                     {tag: 'span', cls: 'tb-line-fn', text: o.filename},

                 ].concat((o.lineno != null) ? [
                     {text: ', line '},
                     {tag: 'span', cls: 'tb-line-line',
                      text: o.lineno + (o.colno == null ? '' : ':' + (o.colno))}
                 ] : [])
                 .concat((o.address != null) ? [
                     {text: ', at '},
                     {tag: 'span', cls: 'tb-line-line',
                      text: o.address}
                 ] : [])
                 .concat([
                     {text: ', in '},
                     {tag: 'span', cls: 'tb-line-location', text: o.name}
                 ])},

                {cls: 'tb-line-source collapsed',
                 attrs: {id: id},
                 children: source}
            ]
        };

        if (o.locals) {
            tb_lines.children.push({
                cls: 'tb-locals',

                children: this._render_collapsible({
                    label: 'Locals',
                    collapsed: true,
                    body: this._render(o.locals)
                })
            });
        }

        return tb_lines;
    },

    'lang.Traceback': function(o) {
        var lines = [];

        if (o.items) {
            for (var i = 0; i < o.items.length; i++) {
                lines.push(this._render(o.items[i]));
            }
        }

        var obj = {
            cls: 'exc-traceback',
            children: lines
        };

        return obj;
    },

    'code.Code': function(o) {
        var els = [], i;

        if (o.tokens) {
            for (i = 0; i < o.tokens.length; i++) {
                els.push(this._render(o.tokens[i]));
            }
        }

        return {tag: 'div', cls: 'sx-code', children: els}
    },

    'code.Token': function(o) {
        return {tag: 'span', cls: 'sx-code-token', text: o.val};
    },

    'code.Comment': function(o) {
        return {tag: 'span', cls: 'sx-code-comment', text: o.val};
    },

    'code.Decorator': function(o) {
        return {tag: 'span', cls: 'sx-code-decorator', text: o.val};
    },

    'code.Operator': function(o) {
        return {tag: 'span', cls: 'sx-code-operator', text: o.val};
    },

    'code.String': function(o) {
        return {tag: 'span', cls: 'sx-code-string', text: o.val};
    },

    'code.Number': function(o) {
        return {tag: 'span', cls: 'sx-code-number', text: o.val};
    },

    'code.BuiltinName': function(o) {
        return {tag: 'span', cls: 'sx-code-builtinname', text: o.val};
    },

    'code.ClassName': function(o) {
        return {tag: 'span', cls: 'sx-code-classname', text: o.val};
    },

    'code.FunctionName': function(o) {
        return {tag: 'span', cls: 'sx-code-functionname', text: o.val};
    },

    'code.Constant': function(o) {
        return {tag: 'span', cls: 'sx-code-constant', text: o.val};
    },

    'code.Keyword': function(o) {
        return {tag: 'span', cls: 'sx-code-keyword', text: o.val};
    },

    'code.Punctuation': function(o) {
        return {tag: 'span', cls: 'sx-code-punctuation', text: o.val};
    },

    'code.Tag': function(o) {
        return {tag: 'span', cls: 'sx-code-tag', text: o.val};
    },

    'code.Attribute': function(o) {
        return {tag: 'span', cls: 'sx-code-attribute', text: o.val};
    },

    'Markup': function(o, obj) {
        return {tag: 'span', cls: 'sx-unknown-markup',
                text: ('no renderer for: "' + obj.type + '"')};
    },

    _gen_id: function() {
        this._id++;
        return 'id-' + this._id_base + '-' + this._id;
    },

    _render_long_string: function(o) {
        var id = this._gen_id(),
            detect_code = o.detect_code || false,
            add_long_cls = '',
            text = o.text,
            match;

        if (detect_code) {
            match = text.match(/\n\s{2,}/g);
            if (match && match.length > 3) {
                // at least 4 lines of tabulated text
                add_long_cls += ' sx-pre'
            }
        }

        this.handlers[id + '-handler'] = {
            click: sx.partial(this.on_collapsible_toggler_click, this, id)
        };

        return {
            cls: 'sx-long-str collapsed',

            attrs: {
                id: id
            },

            children: [
                {tag: 'i', cls: 'sx-icon-plus', attrs: {id: id + '-handler'}},

                {tag: 'span', cls: 'sx-long-str-short',
                 text: sx.str.shorten(text, o.maxlen || 100)},

                {tag: 'span', cls: 'sx-long-str-long' + add_long_cls, text: text}
            ]
        };
    },

    _render_collapsible: function(o) {
        var id = this._gen_id();

        this.handlers[id + '-label'] = {
            click: sx.partial(this.on_collapsible_click, this, id)
        };

        var label = o.label;
        if (!sx.is_object(o.label)) {
            label = {text: o.label};
        }

        return {
            cls: 'sx-collapsible' + (o.collapsed ? ' collapsed' : '') + (' ' + (o.cls || '')),

            attrs: {
                id: id
            },

            children: [
                {tag: 'span',
                 cls: 'sx-collapsible-label' + (' ' + (o.label_cls || '')),

                 attrs: {
                     id: id + '-label'
                 },

                 children: [
                     {tag: 'i', cls: 'sx-icon-plus'},
                     label
                 ]},

                 {cls: 'sx-collapsible-body' + (' ' + (o.body_cls || '')), children: o.body}
            ]
        };
    },

    _render_id: function(id) {
        var pid = parseInt(id);
        if (!isNaN(pid)) {
            return '0x' + pid.toString(16);
        }
        return id;
    },

    _render_hr: function(label) {
        return {
            cls: 'hr-caused',
            children: [
                {cls: 'line', children: {tag: 'span', attrs: {style: 'visibility: hidden'},
                                         text: '#'}},

                {cls: 'label', text: label}
            ]
        };
    },

    _render: function(obj) {
        var meth = obj.type;

        if (obj.fields.id) {
            this.objects[obj.fields.id] = obj;
        }

        if (this[meth]) {
            return this[meth].call(this, obj.fields, obj);
        } else {
            var i, meth;

            for (i = 0; i < obj.mro.length; i++) {
                meth = this[obj.mro[i]];
                if (meth) {
                    return meth.call(this, obj.fields, obj)
                }
            }

            return this.Markup(obj.fields, obj);
        }
    },

    _rebind_handlers: function() {
        for (var i in this.handlers) {
            if (this.handlers.hasOwnProperty(i)) {
                var id = i, evs = this.handlers[i];

                for (var j in evs) {
                    if (evs.hasOwnProperty(j)) {
                        sx('#' + id).on(j, evs[j]);
                    }
                }

            }
        }
        this.handlers = {};
    },

    render_spec: function(config) {
        this._render_ctx = config || {};
        var spec = this._render(this.markup);

        try {
            return {cls: 'metamagic-markup', children: spec};
        }
        finally {
            this._render_ctx = null;
        }
    },

    render: function(render_to, config) {
        sx('#' + render_to).update(this.render_spec(config));
        this._rebind_handlers();
    },

    destroy: function(re) {
        this._render_ctx = this.handlers = this.objects = this.markup = null;
    }
};


// Handle conversion from JSON to Markup
var js_id = 0,
    gen_js_id = function() {
        return ++js_id;
    },
    seen_markup = function(obj, seen, refname) {
        // check if we already have this object
        var existing_id = seen.get(obj);

        if (existing_id) {
            return {
                type: 'lang.Ref',
                fields: {
                    ref: existing_id,
                    refname: refname
                },
                mro: ['lang.LangMarkup']
            };
        }
    },
    tos = function(obj) {
        if (obj.toString && sx.is_function(obj.toString)) {
            return obj.toString();
        }
        return Object.prototype.toString.call(obj);
    },
    public_attrs = {
        '$name':1,
        '$cls':1,
        '$mro':1,
        '$module':1
    },
    private_attrs = {
        'toString':1,
        'constructor':1
    },
    public_attr = function(sx_obj_attr) {
        return (sx_obj_attr[0] == '$') ? hop.call(public_attrs, sx_obj_attr) :
                                                    !hop.call(private_attrs, sx_obj_attr);
    };

sx.Markup.Renderer.to_markup = function(obj, seen) {
    // initialize the seen map is needed
    seen = seen || new sx.ds.Map();

    var id = gen_js_id();

    if (sx.is_string(obj) || obj instanceof Date) {
        return {
            type: 'lang.String',
            fields: {str: obj}
        };
    } else if (typeof obj == 'boolean') {
        return {
            type: obj ? 'lang.TrueConstantType' : 'lang.FalseConstantType',
            fields: {}
        };
    } else if (obj == null) {
        return {
            type: 'lang.NoneConstantType',
            fields: {}
        };
    } else if (typeof obj == 'number' || obj instanceof Number) {
        return {
            type: 'lang.Number',
            fields: {num: obj}
        };
    } else if (sx.is_function(obj) && !sx.isinstance(obj, sx.type)) {
        return {
            type: 'code.FunctionName',
            fields: {val: '[Function]'}
        };
    }

    // check if we've seen the obj before, and remember seeing it now
    var prev = seen_markup(obj, seen, tos(obj));
    if (prev) {
        return prev;
    }
    seen.set(obj, id);

    if (sx.is_array(obj)) {
        var items = [];
        for (var i = 0; i < obj.length; i++) {
            items.push(sx.Markup.Renderer.to_markup(obj[i], seen));
        }

        return {
            type: 'lang.List',
            fields: {
                id: id,
                items: items
            }
        };
    } else if (sx.isinstance(obj, [sx.type, sx.object])) {
        var children = [];

        for (var key in obj) {
            if (hop.call(obj, key) && public_attr(key)) {
                children.push({
                    type: 'lang.TreeNodeChild',
                    fields: {
                        id: gen_js_id(),
                        label: key,
                        node: sx.Markup.Renderer.to_markup(obj[key], seen)
                    }
                });
            }
        }

        return {
            type: 'lang.TreeNode',
            fields: {
                id: id,
                name: tos(obj),
                children: children
            }
        };
    } else if (sx.is_object(obj)) {
        var items = {};
        for (var key in obj) {
            if (hop.call(obj, key)) {
                items[key] = sx.Markup.Renderer.to_markup(obj[key], seen);
            }
        }

        return {
            type: 'lang.Dict',
            fields: {
                id: id,
                items: items
            }
        };
    } else {
        throw 'unable to transform to markup: ' + typeof obj
    }
};

sx.Markup.Renderer.unpack_markup = function(packed) {
    var table = packed[0], markup = packed[1], result, ntable = {}, i;

    for (i in table) {
        if (table.hasOwnProperty(i)) {
            ntable[table[i][0]] = {id: i, mro: table[i][1], fields: table[i][2]};
        }
    }

    function _transform(s) {
        if (s == null) {
            return null;
        }

        var i, f, cls_name, fields, result;

        if (sx.is_array(s)) {
            if (s[0] === 0) {
                cls_name = ntable[s[1]].id;
                fields = ntable[s[1]].fields;
                f = {};

                for (i = 0; i < fields.length; i++) {
                    f[fields[i]] = _transform(s[i+2]);
                }

                return {type: cls_name, fields: f, mro: ntable[s[1]].mro};
            }

            if (s[0] === 1) {
                result = [];

                for (i = 1; i < s.length; i++) {
                    result.push(_transform(s[i]));
                }

                return result;
            }

            throw 'unable to serialize: unknown list structure';
        }

        if (sx.is_object(s)) {
            result = {};

            for (i in s) {
                if (s.hasOwnProperty(i)) {
                    result[i] = _transform(s[i]);
                }
            }

            return result;
        }

        return s;
    }

    return _transform(markup);
};


}})(this);
