(function(global) {
    'use strict';

    var assert = {}, undefined = void(0);

    assert.AssertionError = function(msg) {
        this.message = msg;
    };
    assert.AssertionError.prototype = {
        name: 'AssertionError',
        toString: function() {
            return this.name + ': ' + this.message;
        }
    };
    assert.AssertionError.toString = function() {
        return 'AssertionError';
    };

    assert.fail = function(message) {
        throw new assert.AssertionError(message);
    };

    assert.ok = function(value, message) {
        if (!!!value) {
            assert.fail('assert.OK failed: ' + (message || ''));
        }
    };

    assert.not = function(value, message) {
        if (!!value) {
            assert.fail('assert.NOT failed: ' + (message || ''));
        }
    };

    assert.equal = function(arg1, arg2, weak) {
        weak = weak || false;
        if (weak) {
            if (arg1 != arg2) {
                assert.fail('assert.EQUAL failed: ' + arg1 + ' != ' + arg2);
            }
        } else {
            if (arg1 !== arg2) {
                assert.fail('assert.EQUAL failed: ' + arg1 + ' !== ' + arg2);
            }
        }
    };

    assert.raises = function(block, options) {
        var expected = options.error,
            scope = options.scope || global,
            message = options.message || '',
            error_re = options.error_re || null;

        var ex = undefined;
        try {
            block.call(scope);
        } catch (e) {
            ex = e;
        }

        if (ex === undefined) {
            assert.fail('Exception ' + expected + ' was expected to be thrown');
        } else if (!(ex instanceof expected)) {
            assert.fail('Exception ' + expected + ' was expected to be thrown; got: ' + ex);
        }

        if (error_re) {
            var r_error_re = new RegExp(error_re),
                str_ex = String(ex);

            if (!r_error_re.test(str_ex)) {
                assert.fail('Exception message "' + str_ex + '" was expected to match "' +
                             error_re + '"')
            }
        }
    };


    global.assert = assert;
})(typeof window == 'undefined' ? this : window);
