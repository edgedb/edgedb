##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from metamagic.utils.lang.import_ import get_object
from metamagic.utils.lang import context as lang_context
from metamagic.utils.lang.yaml import constructor as yaml_constructor
from metamagic.utils.lang.yaml import loader as yaml_loader
from metamagic.utils.functional import decorate


def raises(ex_cls, ex_msg):
    def dec(func):
        def new(*args, **kwargs):
            slf = args[0]

            constructor = yaml_constructor.Constructor()
            try:
                node = slf.load(func.__doc__)
                node = slf.schema.check(node)
                constructor.construct_document(node)
            except ex_cls as ee:
                assert ex_msg in str(ee), \
                       'expected error "%s" got "%s" instead' % (ex_msg, ee)
            else:
                assert False, 'expected error "%s" got None instead' % ex_msg

        decorate(new, func)
        return new
    return dec


def result(expected_result=None, key=None, value=None):
    def dec(func):
        def new(*args, **kwargs):
            slf = args[0]

            constructor = yaml_constructor.Constructor(context=lang_context.DocumentContext())
            try:
                node = slf.load(func.__doc__)
                node = slf.schema.check(node)
                result = constructor.construct_document(node)
            except Exception:
                raise
            else:
                if key is None:
                    assert expected_result == result, \
                           'unexpected validation result %r, expected %r' % (result, expected_result)
                else:
                    assert result[key] == value, \
                           'unexpected validation result %r, expected %r' % (result[key], value)

        decorate(new, func)
        return new
    return dec


class SchemaTest(object):
    def load(self, str):
        return yaml_loader.Loader(str).get_single_node()

    @staticmethod
    def get_schema(clsname):
        return get_object('metamagic.utils.lang.yaml.validator.tests.ymls.' + clsname)()
