##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


class nested:
    def __init__(self, *managers):
        self.managers = managers

    def __enter__(self):
        vars = []
        entered = []
        error = None

        for manager in self.managers:
            try:
                var = type(manager).__enter__(manager)
            except Exception as e:
                error = e
                break
            else:
                vars.append(var)
                entered.append(manager)
        else:
            return vars

        for manager in reversed(entered):
            if error is None:
                exc_info = (None, None, None)
            else:
                exc_info = (type(error), error, error.__traceback__)

            try:
                stop = type(manager).__exit__(manager, *exc_info)
            except Exception as e:
                e.__cause__ = error
                error = e
            else:
                if stop and error is not None:
                    orig_error = error
                    error = None

        if error is None:
            raise RuntimeError('inhibited error in nested __enter__') from orig_error
        else:
            raise error

    def __exit__(self, error_type, error, error_tb):
        exc_info = (error_type, error, error_tb)
        exit_error = None

        for manager in reversed(self.managers):
            try:
                stop = type(manager).__exit__(manager, *exc_info)
            except Exception as e:
                e.__cause__ = error
                exc_info = (type(e), e, e.__traceback__)
                exit_error = e
            else:
                if stop:
                    exc_info = (None, None, None)

        if exc_info == (None, None, None):
            return True
        elif exit_error is not None:
            raise exit_error

