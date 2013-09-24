/*
* Copyright (c) 2013 Sprymix Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from . import sx, byteutils

(function() {
'use strict';

sx.UUID = function(hex) {
    if (sx.is_string(hex)) {
        this.hex = hex.replace(/[\-\s]/g, '');
    } else {
        this._bytes = hex;
        this.hex = sx.byteutils.hexlify(hex);
    }
};

sx.UUID.prototype.toHex = function() {
    return this.hex;
}

sx.UUID.prototype.toBytes = function() {
    if (this._bytes) {
        return this._bytes;
    }

    this._bytes = sx.byteutils.unhexlify(this.hex);
    return this._bytes;
}

sx.UUID.prototype.toString = sx.UUID.prototype.valueOf = function() {
    if (this._str) {
        return this._str;
    }
    var h = this.hex;
    this._str = h.slice(0, 8) + '-' + h.slice(8, 12) + '-' + h.slice(12, 16) +
                '-' + h.slice(16, 20) + '-' + h.slice(20, 32);
    return this._str;
};

sx.UUID.uuid4 = function() {
    function s(num) {
        return (((1 + Math.random()) * num) | 0).toString(16);
    }

    var hex = s(0x10000000) + '-' + s(0x1000) + '-4' + s(0x100) + '-a' +
                                        s(0x100) + '-' + s(0x1000) + s(0x1000) + s(0x1000);

    return new sx.UUID(hex);
}

})();
