/*
 * Copyright (c) 2012 Sprymix Inc.
 * All rights reserved.
 *
 * See LICENSE for details.
 */


// %import semantix.caos.frontends.javascript.base


sx.caos.register_format('pgjson.', function(format, data, metadata) {
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
