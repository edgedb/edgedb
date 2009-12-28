def decorate(target_func, source_func):
    target_func.__name__ = source_func.__name__
    target_func.__doc__ = source_func.__doc__
    target_func.__dict__.update(source_func.__dict__)


def delegate(*args, **kwargs):
    def decorate(f=None):
        run = None

        args_copy = args
        func = f

        if f is None:
            func = args_copy[0]
            args_copy = args_copy[1:]

        def run():
            return func(*args_copy, **kwargs)

        if f is None:
            return run()
        else:
            decorate.__name__ = func.__name__
            return run

    return decorate

class memoized(object):
    """Decorator that caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned, and
    not re-evaluated.
    """
    def __init__(self, func):
        self.func = func
        self.cache = {}

    def __call__(self, *args):
        try:
            return self.cache[args]
        except KeyError:
            self.cache[args] = value = self.func(*args)
            return value
        except TypeError:
            # uncachable -- for instance, passing a list as an argument.
            # Better to not cache than to blow up entirely.
            return self.func(*args)

    def __repr__(self):
        """Return the function's docstring."""
        return self.func.__doc__
