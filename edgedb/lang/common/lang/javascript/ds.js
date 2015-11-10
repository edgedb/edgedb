/*
* Copyright (c) 2013 MagicStack Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from metamagic.utils.lang.javascript import sx


(function(global) {'use strict'; if (!sx.ds) {


var HASH = 0,
    hop = Object.prototype.hasOwnProperty;

var _key = function(key) {
    var type = typeof key;
    switch (type) {
        case 'number':
            // we support NaN, but don't care about '+0' and '-0'.
        case 'string':
        case 'boolean':
            return type + key;
        case 'undefined':
            return 'undefined';
    }
    if (key === null) {
        return 'null';
    }
    if (key.__class__) { // sx.class, and we're fine with modifying it
        var hash = key.$hash;
        if (hash != null) {
            return hash;
        }
        return (key.$hash = ('hash' + (++HASH)));
    }
    return null;
};


var _clone = function(obj) {
    function F() {};
    F.prototype = obj;
    return new F();
};


if (!global.Map) {
    var Map = function() {
        this.size = 0;
        this._hash = {};
        this._keys = [];
        this._values = [];
    };

    Map.prototype = {
        set: function(key, value) {
            var hashed = _key(key);
            if (hashed === null) {
                this._keys.push(key);
                this._values.push(value);
            } else {
                this._hash[hashed] = value;
            }
            this.size++;
        },

        get: function(key) {
            var hashed = _key(key);
            if (hashed === null) {
                var idx = sx.array.index(this._keys, key);
                if (idx >= 0) {
                    return this._values[idx];
                }
            } else {
                return this._hash[hashed];
            }
        },

        has: function(key) {
            var hashed = _key(key);
            if (hashed === null) {
                return sx.array.index(this._keys, key) >= 0;
            } else {
                return hop.call(this._hash, hashed);
            }
        },

        clear: function() {
            this.size = 0;
            this._hash = {};
            this._keys = [];
            this._values = [];
        },

        del: function(key) {
            var hashed = _key(key);
            if (hashed === null) {
                var idx = sx.array.index(this._keys, key);
                if (idx >= 0) {
                    this._keys.splice(idx, 1);
                    this._values.splice(idx, 1);
                    this.size--;
                    return true;
                }
            } else {
                if (hop.call(this._hash, hashed)) {
                    delete this._hash[hashed];
                    this.size--;
                    return true;
                }
            }
            return false;
        }
    };
} else {
    var Map = function() {
        this.map = new global.Map();
    };
    Map.prototype = {
        set: function(key, value) {
            return this.map.set(key, value);
        },
        get: function(key) {
            return this.map.get(key);
        },
        has: function(key) {
            return this.map.has(key);
        },
        clear: function() {
            return this.map.clear();
        },
        del: function(key) {
            return this.map['delete'].call(this.map, key);
        }
    };
    Object.defineProperty(Map.prototype, 'size', {
        get: function() {
            return this.map.size;
        }
    });
}

if (!global.Set) {
    var Set = function() {
        this.size = 0;
        this._hash = {};
        this._items = [];
    };

    Set.prototype = {
        add: function(item) {
            var hashed = _key(item);
            if (hashed === null) {
                this._items.push(item);
            } else {
                this._hash[hashed] = true;
            }
            this.size++;
        },

        has: function(item) {
            var hashed = _key(item);
            if (hashed === null) {
                return sx.array.index(this._items, item) >= 0;
            } else {
                return hop.call(this._hash, hashed);
            }
        },

        clear: function() {
            this.size = 0;
            this._hash = {};
            this._items = [];
        },

        del: function(item) {
            var hashed = _key(item);
            if (hashed === null) {
                var idx = sx.array.index(this._keys, item);
                if (idx >= 0) {
                    this._items.splice(idx, 1);
                    this.size--;
                    return true;
                }
            } else {
                if (hop.call(this._hash, hashed)) {
                    delete this._hash[hashed];
                    this.size--;
                    return true;
                }
            }
            return false;
        }
    };
} else {
    var Set = function() {
        this.set = new global.Set();
    };
    Set.prototype = {
        add: function(item) {
            return this.set.add(item);
        },
        has: function(item) {
            return this.set.has(item);
        },
        clear: function() {
            return this.set.clear();
        },
        del: function(item) {
            return this.set['delete'].call(this.set, item);
        }
    };
    Object.defineProperty(Set.prototype, 'size', {
        get: function() {
            return this.set.size;
        }
    });
}

sx.ds = {
    Map: Map,
    Set: Set
};

}})(this);
