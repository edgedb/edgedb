/*
 * Copyright (c) 2012 Sprymix Inc.
 * All rights reserved.
 *
 * See LICENSE for details.
 */


// %import semantix.utils.lang.javascript.types
// %import semantix.caos.frontends.javascript.base


sx.types.register('pgjson.', function(format, data, metadata) {
    "use strict";

    var format_string = format[0], format_version = format[1];
    var FREEFORM_RECORD_ID = '6e51108d-7440-47f7-8c65-dc4d43fd90d2';

    var _throw = function(msg) {
        throw new sx.Error('malformed "' + format_string + '" data: ' + msg);
    }

    if (format_string == 'pgjson.caos.selector' || format_string == 'pgjson.caos.queryselector') {
        if (format_version != 1) {
            throw new sx.Error('unsupported "' + format_string + '" version: ' + format_version);
        }
    } else {
        _throw('unsupported format: "' + format_string + '"');
    }

    if (!sx.is_array(data)) {
        _throw('not an array');
    }

    var result = [];
    var record_info = {};

    for (var i = 0; i < metadata.record_info.length; i++) {
        var ri = metadata.record_info[i];
        record_info[ri.id] = ri;
    }

    var _decode_record_tree = function(tree, connecting_attribute) {
        var tl = tree.length;

        var toplevel = [];
        var updates = {};
        var ca = connecting_attribute;
        var attrname = ca.name + ca.direction + ca.target;
        var target;

        var total_order = {};

        var items = {};

        for (var i = 0; i < tl; i++) {
            var item = _decode_record(tree[i]);
            var item_id = item[ca.name].id;
            items[item_id] = item;
            total_order[item_id] = i;
        }

        for (var id in items) {
            if (!items.hasOwnProperty(id)) {
                continue;
            }

            var item = items[id];

            var entity = item[ca.name];

            if (entity == null) {
                continue;
            }

            var target_id = item['__target__'];

            if (target_id == null) {
                continue;
            }

            var target = items[target_id];

            if (!target) {
                // The items below us have been cut off by recursion depth limit
                continue;
            }

            var titem = [total_order[target_id], target[ca.name]];

            if (item['__depth__'] == 0) {
                sx.array.insort_left(toplevel, titem);
            } else {
                if (!updates.hasOwnProperty(entity.id)) {
                    updates[entity.id] = [titem];
                } else {
                    sx.array.insort_left(updates[entity.id], titem);
                }
            }
        }

        for (var src_id in updates) {
            if (updates.hasOwnProperty(src_id)) {
                var src = items[src_id][ca.name];
                if (!src) {
                    throw new sx.Error('unexpected error in recursion tree unpack: source is empty');
                }

                var src_updates = updates[src_id];
                var entities = [];

                for (var i = 0; i < src_updates.length; i++) {
                    entities.push(src_updates[i][1]);
                }

                var u = {};
                u[attrname] = entities;

                src.update(u);
            }
        }

        var toplevel_entities = [];

        for (var i = 0; i < toplevel.length; i++) {
            toplevel_entities.push(toplevel[i][1]);
        }

        return toplevel_entities;
    };

    var _decode_record = function(record) {
        var result = {};
        var rec_id = record.f1;
        var rec_info = record_info[rec_id];

        if (rec_id == FREEFORM_RECORD_ID) {
            result = [];

            var reclen = sx.len(record);
            for (var i = 1; i < reclen; i++) {
                var k = 'f' + (i + 1).toString();
                var item_val = record[k];

                if (sx.is_object(item_val) && sx.contains(item_val, 'f1')) {
                    item_val = _decode_record(item_val);
                }

                result.push(item_val);
            }
        } else if (!rec_info) {
            sx.each(record, function(v) {
                var item_key = v.f1;

                if (!item_key) {
                    _throw('missing record item name');
                }

                var item_val = v.f2;

                if (sx.is_object(item_val) && sx.contains(item_val, 'f1')) {
                    item_val = _decode_record(item_val);
                } else if (sx.is_array(item_val)) {
                    var res = [];

                    sx.each(item_val, function(iv) {
                        if (sx.is_object(iv) && sx.contains(iv, 'f1')) {
                            iv = _decode_record(iv);
                        }

                        if (iv != null) {
                            res.push(iv);
                        }
                    });

                    item_val = res;
                }

                result[item_key] = item_val;
            });
        } else {
            if (rec_info.recursive_link) {
                var reclen = sx.len(record);
                var _rec = {};

                for (var i = 1; i < reclen; i++) {
                    var k = 'f' + (i + 1).toString();
                    var item_key = rec_info.attribute_map[i - 1];

                    var item_val = record[k];

                    _rec[item_key] = item_val;
                }

                if (_rec['data']) {
                    return _decode_record_tree(_rec['data'], rec_info.recursive_link);
                }
            }

            var reclen = sx.len(record);

            for (var i = 1; i < reclen; i++) {
                var k = 'f' + (i + 1).toString();
                var item_key = rec_info.attribute_map[i - 1];

                if (!item_key) {
                    _throw('missing record item name');
                }

                var item_val = record[k];

                if (sx.is_object(item_val) && sx.contains(item_val, 'f1')) {
                    item_val = _decode_record(item_val);
                } else if (sx.is_array(item_val)) {
                    var res = [];

                    sx.each(item_val, function(iv) {
                        if (sx.is_object(iv) && sx.contains(iv, 'f1')) {
                            iv = _decode_record(iv);
                        }

                        if (iv != null) {
                            res.push(iv);
                        }
                    });

                    item_val = res;
                }

                if (sx.is_object(item_key)) {
                    // key is a PointerVector
                    if (item_key.direction == '<') {
                        result[item_key.name + item_key.direction + item_key.target] = item_val;
                    } else {
                        result[item_key.name] = item_val;
                    }
                } else {
                    result[item_key] = item_val;
                }
            }
        }

        if (result['$sxclsid$']) {
            delete result['$sxclsid$'];
        }

        if (result['$sxclsname$']) {
            var clsname = result['$sxclsname$'];
            delete result['$sxclsname$'];
            var cls = sx.caos.schema.get(clsname);

            // Filter out pointers that do not belong to this class.
            // This happens when we receive a combined record for multiple classes.
            var keys = sx.keys(result), kl = keys.length;

            for (var i = 0; i < kl; i++) {
                var key = keys[i];
                if (!sx.contains(key, '<') && !cls['$ptr$' + key]) {
                    delete result[key];
                }
            }

            result = new cls(result);
        } else if (result.hasOwnProperty('t')) {
            result = new sx.caos.xvalue(result['t'], result['p']);
        }

        return result;
    };

    if (format_string == 'pgjson.caos.selector') {
        for (var i = 0; i < data.length; i++) {
            var item = _decode_record(data[i]);

            if (sx.len(item) != 1) {
                _throw('top-level element must contain exactly one attribute');
            }

            result.push(sx.first(item));
        }
    } else {
        for (var i = 0; i < data.length; i++) {
            var item = _decode_record(data[i]);
            result.push(item);
        }
    }

    return result;
});
