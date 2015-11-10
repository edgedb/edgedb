/*
* Copyright (c) 2013 MagicStack Inc.
* All rights reserved.
*
* See LICENSE for details.
**/


// %from metamagic.utils.lang.javascript import sx, class


(function(global) {'use strict'; if (!sx.common) {


var Observable = sx.define('sx.common.Observable', [], {
    constructor: function() {
        sx.parent(Observable, this, 'constructor', arguments);
        this._event_handlers = {};
    },

    on: function(event, handler, scope) {
        if (!this._event_handlers[event]) {
            this._event_handlers[event] = [];
        }

        this._event_handlers[event].push([handler, scope || global]);
    },

    fire: function() {
        var self = this, args = arguments;
        setTimeout(function() { // to prevent possible infinite recursion
            self._fire.apply(self, args);
        }, 0);
    },

    _fire: function(event/*, args */) {
        var handlers = this._event_handlers[event], i;

        if (!handlers) {
            return;
        }

        var args = Array.prototype.slice.call(arguments, 1);

        for (i = 0; i < handlers.length; i++) {
            handlers[i][0].apply(handlers[i][1], args);
        }
    }
});


}})(this);
