##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import functools
import unittest


def must_fail(*args, **kwargs):
    def wrap(func):
        _set_spec(func, 'must_fail', (args, kwargs))
        return func
    return wrap


def _set_spec(func, name, attrs):
    try:
        spec = func.test_spec
    except AttributeError:
        spec = func.test_spec = {}

    assert name not in spec
    spec[name] = attrs


class ParserTestMeta(type(unittest.TestCase)):
    def __new__(mcls, name, bases, dct):
        for attr, meth in tuple(dct.items()):
            if attr.startswith('test_') and meth.__doc__:

                @functools.wraps(meth)
                def wrapper(self, meth=meth, doc=meth.__doc__):
                    spec = getattr(meth, 'test_spec', {})
                    spec['test_name'] = meth.__name__

                    if doc:
                        output = error = None

                        source, _, output = doc.partition('\n% OK %')

                        if not output:
                            source, _, error = doc.partition('\n% ERROR %')

                            if not error:
                                output = None
                            else:
                                output = error
                    else:
                        source = output = None

                    self._run_test(source=source, spec=spec, expected=output)

                dct[attr] = wrapper

        return super().__new__(mcls, name, bases, dct)


class BaseParserTest(unittest.TestCase, metaclass=ParserTestMeta):
    def _run_test(self, *, source, spec=None, expected=None):
        if spec and 'must_fail' in spec:
            with self.assertRaises(*spec['must_fail'][0]) as cm:
                return self.run_test(source=source, spec=spec,
                                     expected=expected)

            if cm.exception:
                exc = cm.exception
                for key, val in spec['must_fail'][1].items():
                    if getattr(exc, key) != val:
                        raise exc
            return

        else:
            return self.run_test(source=source, spec=spec, expected=expected)

    def run_test(self, *, source, spec, expected=None):
        raise NotImplementedError
