/**
 * Copyright (c) 2013 Sprymix Inc.
 *
 * All rights reserved.
 * See LICENSE for details.
 */


// %from metamagic.utils.lang.javascript import sx, class, base64, crypt, uuid, byteutils


(function() {
    'use strict';

var _bucket_ids = {};

var BucketMeta = sx.define('metamagic.utils.fs.frontends.javascript.BucketMeta', [sx.type], {
    constructor: function(name, bases, dct) {
        if (this.id) {
            _bucket_ids[this.id.toHex()] = this;
        }
        return sx.parent(BucketMeta, this, 'constructor', arguments);
    },

    statics: {
        get_bucket_class: function(id) {
            id = new sx.UUID(id);
            var bucket = _bucket_ids[id.toHex()];
            if (!bucket) {
                throw new Error('unable to find bucket by id: ' + id);
            }
            return bucket;
        }
    }
});


sx.define('metamagic.utils.fs.frontends.javascript.BaseBucket', [], {
    metaclass: BucketMeta,

    statics: {
        backends: null,

        set_backends: function(backends) {
            this.backends = backends;
        },

        get_backends: function() {
            var mro = this.__mro__,
                len = mro.length,
                i, backends;

            for (i = 0; i < len; i++) {
                backends = mro[i].backends;
                if (backends && backends.length) {
                    return backends;
                }
            }
        },

        get_file_pub_url: function(id, filename) {
            var backends = this.get_backends();

            if (!backends) {
                throw new Error('Bucket.get_file_pub_url: ' + this.__name__ + ' bucket has no ' +
                                'backends set');
            }

            return backends[0].get_file_pub_url(this, id, filename);
        }
    }
});


sx.define('metamagic.utils.fs.frontends.javascript.BaseFSBackend', [], {
    _FN_LEN_LIMIT: 75,

    constructor: function(args) {
        sx.apply(this, args);
    },

    escape_filename: function(filename) {
        return filename.replace(/[^\w\-\._]/g, '_').replace(/^\-\-*/, '').replace(/\-\-*$/, '');
    },

    _get_base_name: function(bucket, id, filename) {
        id = new sx.UUID(id);

        var base = bucket.id.toString(),
            new_id = sx.base64.b32encode(sx.byteutils.unhexlify(sx.crypt.md5(id.toBytes()))),
            base_filename = id.hex;

        if (filename) {
            filename = base_filename + '_' + filename;
        } else {
            filename = base_filename;
        }

        if (filename.length > this._FN_LEN_LIMIT) {
            if (filename.indexOf('.') > -1) {

                var extension = sx.str.rpartition(filename, '.')[2];
                var limit = this._FN_LEN_LIMIT - extension.length - 1;

                if (limit <= 0) {
                    filename = filename.slice(0, this._FN_LEN_LIMIT);
                } else {
                    filename = filename.slice(0, limit) + '.' + extension;
                }
            } else {
                filename = filename.slice(0, this._FN_LEN_LIMIT);
            }

        }

        return base + '/' + new_id.slice(0, 2)
                    + '/' + new_id.slice(2, 4)
                    + '/' + filename;
    },

    get_file_pub_url: function(bucket, id, filename) {
        if (filename) {
            filename = this.escape_filename(filename);
        }
        return this.pub_path + '/' + this._get_base_name(bucket, id, filename);
    }
});


sx.define('metamagic.utils.fs.frontends.javascript.BaseFSSystem', [], {
    constructor: function(args) {
        self.buckets = [];
    },

    add_bucket: function(bucket_cls) {
        self.buckets.push(bucket_cls);
    }
});

})();
