##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import types
import ast
import inspect
import functools

from semantix.utils.lang.python.convert import AstToPyAstConverter, PyAstToGeneric
from semantix.utils.lang.generic.codegen.python import GenericPythonSourceGenerator
from semantix.utils.debug import _indent_code, debug, highlight
from semantix.utils import markup


class GenericLangTestSuiteMeta(type):
    def __new__(mcls, name, bases, dct):
        for method_name, method in dct.items():
            if method_name.startswith('test_') and isinstance(method, types.FunctionType):

                @debug
                def wrapper(method, *args, **kwargs):
                    source = _indent_code(inspect.getsource(method), absolute=0)
                    tree = AstToPyAstConverter.convert(
                               ast.parse(source, method.__code__.co_filename)
                           )

                    """LOG [lang.generic] Python Source
                    print(highlight(source, 'python'))
                    """

                    """LOG [lang.generic] Python Ast Tree
                    markup.dump(tree)
                    """

                    ctree = PyAstToGeneric().convert(tree)

                    """LOG [lang.generic] Generic Ast Tree
                    markup.dump(ctree)
                    """

                    csource = GenericPythonSourceGenerator.to_source(ctree)

                    """LOG [lang.generic] Generic Source to Python
                    print(highlight(csource, 'python'))
                    """

                    result = method(*args, **kwargs)

                    code = compile(ast.parse(csource), '<string>', 'exec')

                    _locals = {}
                    exec(code, _locals, _locals)
                    new_result = _locals[method.__name__](None)

                    assert result == new_result

                def makewrapper(method):
                    def wrap(*args, **kwargs):
                        return wrapper(method, *args, **kwargs)
                    wrap.__name__ = method.__name__
                    wrap.__doc__ = method.__doc__
                    return wrap

                dct[method_name] = makewrapper(method)

        cls = super().__new__(mcls, name, bases, dct)
        return cls


class GenericLangTestSuite(metaclass=GenericLangTestSuiteMeta):
    pass
