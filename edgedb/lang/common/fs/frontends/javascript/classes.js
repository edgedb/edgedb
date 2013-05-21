/**
 * Copyright (c) 2013 Sprymix Inc.
 *
 * All rights reserved.
 * See LICENSE for details.
 */


// %from metamagic.utils.lang.javascript import sx, class, base64, crypt, uuid, byteutils


sx.define('metamagic.utils.fs.frontends.javascript.BaseBucket', [], {
    statics: {
        set_backends: function(backends) {
            this.backends = backends;
        },

        get_backends: function() {
            var mro = this.$mro,
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
                throw new Error('Bucket.get_file_pub_url: ' + this.$name + ' bucket has no ' +
                                'backends set');
            }

            return backends[0].get_file_pub_url(this, id, filename);
        }
    }
});


sx.define('metamagic.utils.fs.frontends.javascript.BaseFSBackend', [], {
    constructor: function(args) {
        sx.apply(this, args);
    },

    escape_filename: function(filename) {
        return filename.replace(/[^\w\-\._]/g, '_').replace(/^\-\-*/, '').replace(/\-\-*$/, '');
    },

    _get_base_name: function(bucket, id, filename) {
        id = new sx.UUID(id);

        var base = bucket.id.toString(),
            new_id = sx.base64.b32encode(sx.byteutils.unhexlify(sx.crypt.md5(id.toBytes())));

        return base + '/' + new_id.slice(0, 2) + '/' + new_id.slice(2, 4) + '/' +
               id.hex + '_' + filename;
    },

    get_file_pub_url: function(bucket, id, filename) {
        filename = this.escape_filename(filename);
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
