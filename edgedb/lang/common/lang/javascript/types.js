/*
 * Copyright (c) 2012 MagicStack Inc.
 * All rights reserved.
 *
 * See LICENSE for details.
 **/


// %from . import sx


sx.types = (function() {
    'use strict';

    var hop = Object.prototype.hasOwnProperty,
        json_parse = sx.json.parse,
        json_formats = [],
        undefined = void(0);

    function _sx_types_walk(ctx, reviver, holder, key) {
        var k, v, value = holder[key];
        if (value && typeof value === 'object') {
            for (k in value) {
                if (hop.call(value, k)) {
                    v = _sx_types_walk(ctx, reviver, value, k);
                    if (v !== undefined) {
                        value[k] = v;
                    } else {
                        delete value[k];
                    }
                }
            }
        }
        return reviver.call(holder, ctx, key, value);
    };

    function _sx_types_unpacker(ctx, data) {
        var data = data.$sxjson$,
            data_format = data.format[0],
            i, len, format;

        for (i = 0, len = json_formats.length; i < len; i++) {
            format = json_formats[i];
            if (sx.str.startswith(data_format, format[0])) {
                return format[1](data.format, data.data, data.metadata, ctx);
            }
        }

        throw new Error('unable to find unpacker for "' + data.format[0] + '"')
    };

    function _sx_types_unpacker_callback(ctx, key, value) {
        if (value != null && hop.call(value, '$sxjson$')) {
            return _sx_types_unpacker(ctx, value);
        }
        return value;
    };

    function sx_types_unpack(data, ctx) {
        return _sx_types_walk(ctx, _sx_types_unpacker_callback, {'': data}, '');
    };

    function sx_types_parse(data, ctx) {
        return json_parse(data, function(key, value) {
            return _sx_types_unpacker_callback(ctx, key, value);
        });
    };

    function sx_types_register(prefix, unpacker) {
        if (json_formats.length == 0) {
            json_formats.push([prefix, unpacker]);
        } else {
            var i, len, item;
            for (i = 0, len = json_formats.length; i < len; i++) {
                item = json_formats[i];

                if (item[0].length > prefix.length || i == (len - 1)) {
                    json_formats.splice(i, 0, [prefix, unpacker]);
                    return;
                }
            }
        }
    };

    return {
        parse: sx_types_parse,
        unpack: sx_types_unpack,
        register: sx_types_register
    };
})();
