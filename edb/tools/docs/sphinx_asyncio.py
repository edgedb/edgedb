# Fork of https://github.com/aio-libs/sphinxcontrib-asyncio
# with latest sphinx compatibility.

# Copyright 2020 aio-libs collaboration.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from docutils import nodes
from docutils.parsers.rst import directives
from sphinx import addnodes
from sphinx.domains.python import PyFunction, PyMethod
from sphinx.ext.autodoc import FunctionDocumenter, MethodDocumenter, \
    bool_option
try:
    from asyncio import iscoroutinefunction
except ImportError:
    def iscoroutinefunction(func):
        """Return True if func is a decorated coroutine function."""
        return getattr(func, '_is_coroutine', False)


def merge_dicts(*dcts):
    ret = {}
    for d in dcts:
        for k, v in d.items():
            ret[k] = v
    return ret


class PyCoroutineMixin(object):
    option_spec = {'coroutine': directives.flag,
                   'async-with': directives.flag,
                   'async-for': directives.flag}

    def get_signature_prefix(self, sig):
        ret = []
        if 'staticmethod' in self.options:
            ret += [nodes.Text('staticmethod'), addnodes.desc_sig_space()]
        if 'classmethod' in self.options:
            ret += [nodes.Text('classmethod'), addnodes.desc_sig_space()]
        if 'coroutine' in self.options:
            coroutine = True
        else:
            coroutine = ('async-with' not in self.options and
                         'async-for' not in self.options)
        if coroutine:
            ret += [nodes.Text('coroutine'), addnodes.desc_sig_space()]
        if 'async-with' in self.options:
            ret += [nodes.Text('async-with'), addnodes.desc_sig_space()]
        if 'async-for' in self.options:
            ret += [nodes.Text('async-for'), addnodes.desc_sig_space()]
        return ret


class PyCoroutineFunction(PyCoroutineMixin, PyFunction):
    option_spec = merge_dicts(PyCoroutineMixin.option_spec,
                              PyFunction.option_spec)

    def run(self):
        self.name = 'py:function'
        return super(PyCoroutineFunction, self).run()


class PyCoroutineMethod(PyCoroutineMixin, PyMethod):
    option_spec = merge_dicts(PyCoroutineMixin.option_spec,
                              PyMethod.option_spec,
                              {'staticmethod': directives.flag,
                               'classmethod': directives.flag})

    def run(self):
        self.name = 'py:method'
        return super(PyCoroutineMethod, self).run()


class CoFunctionDocumenter(FunctionDocumenter):
    """
    Specialized Documenter subclass for functions and coroutines.
    """
    objtype = "cofunction"
    directivetype = "cofunction"
    priority = 2
    option_spec = merge_dicts(
        MethodDocumenter.option_spec,
        {'async-with': bool_option,
         'async-for': bool_option,
         'coroutine': bool_option
         })

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        """Called to see if a member can be documented by this documenter."""
        if not super().can_document_member(member, membername, isattr, parent):
            return False
        return iscoroutinefunction(member)

    def add_directive_header(self, sig):
        super().add_directive_header(sig)
        sourcename = self.get_sourcename()
        if self.options.async_with:
            self.add_line('   :async-with:', sourcename)
        if self.options.async_for:
            self.add_line('   :async-for:', sourcename)
        if self.options.coroutine:
            self.add_line('   :coroutine:', sourcename)


class CoMethodDocumenter(MethodDocumenter):
    """
    Specialized Documenter subclass for methods and coroutines.
    """
    objtype = "comethod"
    priority = 3  # Higher than CoFunctionDocumenter
    option_spec = merge_dicts(
        MethodDocumenter.option_spec,
        {'staticmethod': bool_option,
         'classmethod': bool_option,
         'async-with': bool_option,
         'async-for': bool_option,
         'coroutine': bool_option
         })

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        """Called to see if a member can be documented by this documenter."""
        if not super().can_document_member(member, membername, isattr, parent):
            return False
        return iscoroutinefunction(member)

    def import_object(self):
        ret = super().import_object()
        # Was overridden by method documenter, return to default
        self.directivetype = "comethod"
        return ret

    def add_directive_header(self, sig):
        super().add_directive_header(sig)
        sourcename = self.get_sourcename()
        if self.options.staticmethod:
            self.add_line('   :staticmethod:', sourcename)
        if self.options.staticmethod:
            self.add_line('   :classmethod:', sourcename)
        if self.options.async_with:
            self.add_line('   :async-with:', sourcename)
        if self.options.async_for:
            self.add_line('   :async-for:', sourcename)
        if self.options.coroutine:
            self.add_line('   :coroutine:', sourcename)


def setup(app):
    app.add_directive_to_domain('py', 'coroutinefunction', PyCoroutineFunction)
    app.add_directive_to_domain('py', 'coroutinemethod', PyCoroutineMethod)
    app.add_directive_to_domain('py', 'corofunction', PyCoroutineFunction)
    app.add_directive_to_domain('py', 'coromethod', PyCoroutineMethod)
    app.add_directive_to_domain('py', 'cofunction', PyCoroutineFunction)
    app.add_directive_to_domain('py', 'comethod', PyCoroutineMethod)

    app.add_autodocumenter(CoFunctionDocumenter)
    app.add_autodocumenter(CoMethodDocumenter)
    return {'version': '1.0', 'parallel_read_safe': True}
