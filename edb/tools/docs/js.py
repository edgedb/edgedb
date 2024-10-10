#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


r"""
===========================================
:js: domain for Gel driver documentation
===========================================

This module extends the default js domain by overriding class, method
and function directives in the following ways:

* The main change is that TypeScript-like specifications are now
  supported in signatures, allowing a better way to specify what the
  expected arguments and return types are supposed to be. The types
  are optional.

* A :staticmethod: option is added to the methods.

* Class signatures support `extends` clause.

* The :param: option supports type specification without trying to
  link to the types (since mostly they are native JS types that don't
  necessarily have a meaningful link).
"""

from __future__ import annotations

from docutils import nodes as d_nodes
from docutils.parsers.rst import directives  # type: ignore

import pygments.lexers

from sphinx import addnodes as s_nodes
from sphinx.domains import javascript as js
from sphinx.locale import _
from sphinx.util import docfields


class JSFieldMixin:
    def make_xref(self, rolename, domain, target, *args, **kwargs):
        if rolename:
            return d_nodes.literal(target, target)
        return super().make_xref(rolename, domain, target, *args, **kwargs)


class JSTypedField(JSFieldMixin, docfields.TypedField):
    pass


class JSCallableDirective(js.JSCallable):
    doc_field_types = [  # type: ignore
        JSTypedField('arguments', label=_('Arguments'),
                     names=('argument', 'arg', 'parameter', 'param'),
                     typerolename='func', typenames=('paramtype', 'type')),
    ] + js.JSCallable.doc_field_types[1:]   # type: ignore

    def handle_signature(self, sig, signode):
        # if the function has a return type specified, clip it before
        # processing the rest of signature
        if sig[-1] != ')' and '):' in sig:
            newsig, rettype = sig.rsplit(':', 1)
            rettype = rettype.strip()
        else:
            newsig = sig
            rettype = None

        fullname, prefix = super().handle_signature(newsig, signode)

        if rettype:
            signode += s_nodes.desc_returns(rettype, rettype)

        return fullname, prefix


class JSMethodDirective(JSCallableDirective):
    option_spec = {
        **js.JSCallable.option_spec,
        **{'staticmethod': directives.flag},
    }

    def handle_signature(self, sig, signode):
        fullname, prefix = super().handle_signature(sig, signode)

        if 'staticmethod' in self.options:
            signode.insert(
                0, s_nodes.desc_annotation('static method', 'static method'))

        return fullname, prefix


class JSClassDirective(JSCallableDirective):
    """Like a callable but with an optional "extends" clause."""
    display_prefix = 'class '
    allow_nesting = True

    def handle_signature(self, sig, signode):
        # if the class has "extends" clause specified, clip it before
        # processing the rest of signature
        if ' extends ' in sig:
            newsig, mro = sig.rsplit(' extends ', 1)
            mro = mro.strip()
            newsig = newsig.strip()
        else:
            newsig = sig
            mro = None

        fullname, prefix = super().handle_signature(newsig, signode)

        if mro:
            signode['mro'] = mro
            mronode = s_nodes.desc_type('extends', '')
            signode += mronode
            for itype in mro.split(','):
                itype = itype.strip()
                mronode += s_nodes.desc_type(itype, itype)

        return fullname, prefix


class JSDomain(js.JavaScriptDomain):
    directives = {
        **js.JavaScriptDomain.directives,
        **{
            'function': JSCallableDirective,
            'method': JSMethodDirective,
            'class': JSClassDirective,
        }
    }


def setup_domain(app):
    # Dummy lexers; the actual highlighting is implemented
    # in the edgedb.com website code.
    app.add_lexer("tsx", pygments.lexers.TextLexer)
    app.add_lexer("tsx-diff", pygments.lexers.TextLexer)
    app.add_lexer("typescript-diff", pygments.lexers.TextLexer)
    app.add_lexer("javascript-diff", pygments.lexers.TextLexer)

    app.add_domain(JSDomain, override=True)
