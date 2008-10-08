Semantix.Bubbling = function() {

    this.events = {};

    this.on = function(event, handler, scope) {
        if (scope) {
            handler = handler.createDelegate(scope);
        }

        if (!this.events.hasOwnProperty(event)) {
            this.events[event] = [];
        }

        this.events[event].push(handler);
    };

    this.fire = function(event, package) {
        if (this.events.hasOwnProperty(event)) {
            for (var i in this.events[event]) {
                this.events[event][i](package);
            }
        }
    };

    return this;
}();
