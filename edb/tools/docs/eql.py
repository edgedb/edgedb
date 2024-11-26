#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2018-present MagicStack Inc. and the EdgeDB authors.
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
=====================================
:eql: domain for EdgeQL documentation
=====================================


Functions
---------

To declare a function use a ".. eql:function::" directive.  A few
things must be defined:

* Full function signature with a fully qualified name must be specified.

* ":param $name: description:" a short description of the $name parameter.
  $name must match the the name of the parameter in function's signature.
  If a parameter is anonymous, its number should be used instead (e.g. $1).

* ":paramtype $name: type": for every :param: there must be a
  corresponding :paramtype field.  For example: ":paramtype $name: int64"
  declares that the type of the $name parameter is `int64`.  If a parameter
  has more than one valid types list them separated by "or":
  ":paramtype $name: int64 or str".

* :return: and :returntype: are similar to :param: and
  :paramtype: but lack parameter names.  They must be used to document
  the return value of the function.

* A few paragraphs and code samples.  The first paragraph must
  be a single sentence no longer than 79 characters describing the
  function.

Example:

    .. eql:function:: std::array_agg(SET OF any, $a: any) -> array<any>

        :param $1: input set
        :paramtype $1: SET OF any

        :param $a: description of this param
        :paramtype $a: int64 or str

        :return: array made of input set elements
        :returntype: array<any>

        Return the array made from all of the input set elements.

        The ordering of the input set will be preserved if specified.


A function can be referenced from anywhere in the documentation by using
a ":eql:func:" role.  For instance:

* ":eql:func:`array_agg`";
* ":eql:func:`std::array_agg`";
* or, "look at this :eql:func:`fancy function <array_agg>`".


Operators
---------

Use ".. eql:operator::" directive to declare an operator.  Supported fields:

* ":optype NAME: TYPE" -- operand type.

The first argument of the directive must be a string in the following
format: "OPERATOR_ID: OPERATOR SIGNATURE".  For instance, for a "+"
operator it would be "PLUS: A + B":

    .. eql:operator:: PLUS: A + B

        :optype A: int64 or str or bytes
        :optype B: any
        :resulttype: any

        Arithmetic addition.

To reference an operator use the :eql:op: role along with OPERATOR_ID:
":eql:op:`plus`" or ":eql:op:`+ <plus>`".  Operator ID is case-insensitive.


Statements
----------

Use ":eql-statement:" field for sections that describe a statement.

A :eql-haswith: field should be used if the statement supports a WITH block.

Example:

    SELECT
    ------

    :eql-statement:
    :eql-haswith:

    SELECT is used to select stuff.


    .. eql:synopsis::

        [WITH [MODULE name]]
        SELECT expr
        FILTER expr


    .. eql:clause:: FILTER: A FILTER B

        :paramtype A: any
        :paramtype B: SET OF any
        :returntype: any

        FILTER should be used to filter stuff.


    More paragraphs describing intricacies of SELECT go here...

    More paragraphs describing intricacies of SELECT go here...

    More paragraphs describing intricacies of SELECT go here...

Notes:

* To reference a statement use the ":eql:stmt:" role.  For instance:

  - :eql:stmt:`SELECT`
  - :eql:stmt:`my fav statement <SELECT>`
  - :eql:stmt:`select`
  - :eql:stmt:`CREATE FUNCTION`
  - :eql:stmt:`create function <CREATE FUNCTION>`

* Synopsis section, denoted with ".. eql:synopsis::" should follow the
  format used in PostgreSQL documentation:
  https://www.postgresql.org/docs/10/static/sql-select.html

* An "inline-synopsis" role can be used as an inline highlighted code block:

  - :eql:inline-synopsis:`ADD ATTRIBUTE <attribute_name>`.


Types
-----

To declare a type use a ".. eql:type::" directive.  It doesn't have any
fields at the moment, just description.  Example:

    .. eql:type:: std::bytes

        A sequence of bytes.

To reference a type use a ":eql:type:" role, e.g.:

- :eql:type:`bytes`
- :eql:type:`std::bytes`
- :eql:type:`SET OF any`
- :eql:type:`SET OF array\<any\>`
- :eql:type:`array of \<int64\> <array<int64>>`
- :eql:type:`array\<int64\>`

Keywords
--------

To describe a keyword use a ".. eql:keyword::" directive.  Example:

    .. eql:keyword:: WITH

        The ``WITH`` block in EdgeQL is used to define aliases.

If a keyword is compound use dash to separate keywords:


    .. eql:keyword:: SET-OF

To reference a keyword use a ":eql:kw:" role.  For instance:

* :eql:kw:`WITH block <with>`
* :eql:kw:`SET OF <SET-OF>`

"""


from __future__ import annotations

import io
import importlib
import re

import lxml.etree
import pygments.lexers
import pygments.lexers.special

from typing import Any, Dict

from edb.common import debug

from edb.tools.pygments.edgeql import EdgeQLLexer

from edb import protocol

from docutils import nodes as d_nodes
from docutils.parsers import rst as d_rst
from docutils.parsers.rst import directives as d_directives  # type: ignore
from docutils import utils as d_utils

from sphinx import addnodes as s_nodes
from sphinx import directives as s_directives
from sphinx import domains as s_domains
from sphinx import roles as s_roles
from sphinx import transforms as s_transforms
from sphinx.util import docfields as s_docfields
from sphinx.util import nodes as s_nodes_utils
from sphinx.ext.intersphinx import InventoryAdapter

from . import shared


class EQLField(s_docfields.Field):

    def __init__(
        self,
        name,
        names=(),
        label=None,
        has_arg=False,
        rolename=None,
        bodyrolename=None,
    ):
        super().__init__(name, names, label, has_arg, rolename, bodyrolename)

    def make_field(self, *args, **kwargs):
        node = super().make_field(*args, **kwargs)
        node['eql-name'] = self.name
        return node

    def make_xref(
        self,
        rolename,
        domain,
        target,
        innernode=d_nodes.emphasis,
        contnode=None,
        env=None,
        inliner=None,
        location=None,
    ):

        if not rolename:
            return contnode or innernode(target, target)

        title = target
        if domain == 'eql' and rolename == 'type':
            target = EQLTypeXRef.filter_target(target)
            if target in EQLTypedField.ignored_types:
                return d_nodes.Text(title)

        refnode = s_nodes.pending_xref('', refdomain=domain,
                                       refexplicit=title != target,
                                       reftype=rolename, reftarget=target,
                                       location=location)
        refnode += contnode or innernode(title, title)
        if env:
            env.domains[domain].process_field_xref(refnode)

        refnode['eql-auto-link'] = True
        return refnode

    def make_xrefs(
        self,
        rolename,
        domain,
        target,
        innernode=d_nodes.emphasis,
        contnode=None,
        env=None,
        inliner=None,
        location=None,
    ):
        delims = r'''(?x)
        (
            \s* [\[\]\(\)<>,] \s* | \s+or\s+ |
            \s*\bSET\s+OF\s+ |
            \s*\bOPTIONAL\s+
        )
        '''

        delims_re = re.compile(delims)
        sub_targets = re.split(delims, target)

        split_contnode = bool(contnode and contnode.astext() == target)

        results = []
        for sub_target in filter(None, sub_targets):
            if split_contnode:
                contnode = d_nodes.Text(sub_target)

            if delims_re.match(sub_target):
                results.append(contnode or innernode(sub_target, sub_target))
            else:
                results.append(self.make_xref(rolename, domain, sub_target,
                                              innernode, contnode, env,
                                              inliner=inliner,
                                              location=location))

        return results


INDEX_FIELD = EQLField(
    'index',
    label='Index Keywords',
    names=('index',),
    has_arg=False)


class EQLTypedField(EQLField):

    ignored_types = {
        'type'
    }

    def __init__(
        self,
        name,
        names=(),
        label=None,
        rolename=None,
        *,
        typerolename,
        has_arg=True,
    ):
        super().__init__(name, names, label, has_arg, rolename, None)
        self.typerolename = typerolename

    def make_field(
        self, types, domain, item, env=None, inliner=None, location=None
    ):
        fieldarg, fieldtype = item

        body = d_nodes.paragraph()
        if fieldarg:
            body.extend(self.make_xrefs(self.rolename, domain, fieldarg,
                                        s_nodes.literal_strong, env=env,
                                        inliner=inliner, location=location))

            body += d_nodes.Text('--')

        typename = u''.join(n.astext() for n in fieldtype)
        body.extend(
            self.make_xrefs(self.typerolename, domain, typename,
                            s_nodes.literal_emphasis, env=env,
                            inliner=inliner, location=location))

        fieldname = d_nodes.field_name('', self.label)
        fieldbody = d_nodes.field_body('', body)

        node = d_nodes.field('', fieldname, fieldbody)
        node['eql-name'] = self.name
        node['eql-opname'] = fieldarg
        if typename:
            node['eql-optype'] = typename
        return node


class EQLTypedParamField(EQLField):

    is_typed = True

    def __init__(
        self,
        name,
        names=(),
        label=None,
        rolename=None,
        *,
        has_arg=True,
        typerolename,
        typenames,
    ):
        super().__init__(name, names, label, has_arg, rolename)
        self.typenames = typenames
        self.typerolename = typerolename

    def make_field(self, types, domain, item, env=None, inliner=None):
        fieldname = d_nodes.field_name('', self.label)

        fieldarg, content = item
        body = d_nodes.paragraph()
        body.extend(self.make_xrefs(self.rolename, domain, fieldarg,
                                    s_nodes.literal_strong, env=env,
                                    inliner=inliner))

        typename = None
        if fieldarg in types:
            body += d_nodes.Text(' (')

            # NOTE: using .pop() here to prevent a single type node to be
            # inserted twice into the doctree, which leads to
            # inconsistencies later when references are resolved
            fieldtype = types.pop(fieldarg)
            if len(fieldtype) == 1 and isinstance(fieldtype[0], d_nodes.Text):
                typename = u''.join(n.astext() for n in fieldtype)
                body.extend(
                    self.make_xrefs(self.typerolename, domain, typename,
                                    s_nodes.literal_emphasis, env=env,
                                    inliner=inliner))
            else:
                body += fieldtype
            body += d_nodes.Text(')')

        body += d_nodes.Text(' -- ')
        body += content

        fieldbody = d_nodes.field_body('', body)

        node = d_nodes.field('', fieldname, fieldbody)
        node['eql-name'] = self.name
        node['eql-paramname'] = fieldarg
        if typename:
            node['eql-paramtype'] = typename
        return node


class BaseEQLDirective(s_directives.ObjectDescription):

    @staticmethod
    def strip_ws(text):
        text = text.strip()
        text = ' '.join(
            line.strip() for line in text.split() if line.strip())
        return text

    def _validate_and_extract_summary(self, node):
        desc_cnt = None
        for child in node.children:
            if isinstance(child, s_nodes.desc_content):
                desc_cnt = child
                break
        if desc_cnt is None or not desc_cnt.children:
            raise self.error('the directive must include a description')

        first_node_index = 0
        first_node = desc_cnt.children[first_node_index]

        if isinstance(first_node, d_nodes.field_list):
            if len(desc_cnt.children) < 2:
                raise self.error('the directive must include a description')

            first_node_index += 1
            first_node = desc_cnt.children[first_node_index]

        if isinstance(first_node, s_nodes.versionmodified):
            first_node_index += 1
            first_node = desc_cnt.children[first_node_index]

        if not isinstance(first_node, d_nodes.paragraph):
            raise self.error(
                'there must be a short text paragraph after directive fields')

        summary = self.strip_ws(first_node.astext())
        if len(summary) > 79:
            raise self.error(
                f'First paragraph is expected to be shorter than 80 '
                f'characters, got {len(summary)}: {summary!r}')

        node['summary'] = summary

    def _find_field_desc(self, field_node: d_nodes.field):
        fieldname = field_node.children[0].astext()

        if ' ' in fieldname:
            fieldtype, fieldarg = fieldname.split(' ', 1)
            fieldarg = fieldarg.strip()
            if not fieldarg:
                fieldarg = None
        else:
            fieldtype = fieldname
            fieldarg = None

        fieldtype = fieldtype.lower().strip()

        for fielddesc in self.doc_field_types:
            if fielddesc.name == fieldtype:
                return fieldtype, fielddesc, fieldarg

        return fieldtype, None, fieldarg

    def _validate_fields(self, node):
        desc_cnt = None
        for child in node.children:
            if isinstance(child, s_nodes.desc_content):
                desc_cnt = child
                break
        if desc_cnt is None or not desc_cnt.children:
            raise self.error('the directive must include a description')

        fields = None
        first_node = desc_cnt.children[0]
        if isinstance(first_node, d_nodes.field_list):
            fields = first_node

        for child in desc_cnt.children[1:]:
            if isinstance(child, d_nodes.field_list):
                raise self.error(
                    f'fields must be specified before all other content')

        if fields:
            for field in fields:
                if 'eql-name' in field:
                    continue

                # Since there is *no* validation or sane error reporting
                # in Sphinx, attempt to do it here.

                fname, fdesc, farg = self._find_field_desc(field)
                msg = f'found unknown field {fname!r}'

                if fdesc is None:
                    msg += (
                        f'\n\nPossible reason: field {fname!r} '
                        f'is not supported by the directive; '
                        f'is there a typo?\n\n'
                    )
                else:
                    if farg and not fdesc.has_arg:
                        msg += (
                            f'\n\nPossible reason: field {fname!r} '
                            f'is specified with an argument {farg!r}, but '
                            f'the directive expects it without one.\n\n'
                        )
                    elif not farg and fdesc.has_arg:
                        msg += (
                            f'\n\nPossible reason: field {fname!r} '
                            f'expects an argument but did not receive it;'
                            f'check your ReST source.\n\n'
                        )

                raise self.error(msg)

    def run(self):
        indexnode, node = super().run()
        self._validate_fields(node)
        self._validate_and_extract_summary(node)

        objects = self.env.domaindata['eql']['objects']
        objects[self.__eql_target] += (node['summary'],)

        return [indexnode, node]

    def add_target_and_index(self, name, sig, signode):
        target = name.replace(' ', '-')

        if target in self.state.document.ids:
            raise self.error(
                f'duplicate {self.objtype} {name} description')

        signode['names'].append(target)
        signode['ids'].append(target)
        signode['first'] = (not self.names)
        self.state.document.note_explicit_target(signode)

        objects = self.env.domaindata['eql']['objects']

        if target in objects:
            raise self.error(
                f'duplicate {self.objtype} {name} description')
        objects[target] = (self.env.docname, self.objtype)

        self.__eql_target = target


class EQLTypeDirective(BaseEQLDirective):

    doc_field_types = [
        INDEX_FIELD,
    ]

    def handle_signature(self, sig, signode):
        if '::' in sig:
            mod, name = sig.strip().rsplit('::', 1)
        else:
            name = sig.strip()
            mod = 'std'

        display = name.replace('-', ' ')
        if mod != 'std':
            display = f'{mod}::{display}'

        signode['eql-module'] = mod
        signode['eql-name'] = name
        signode['eql-fullname'] = fullname = f'{mod}::{name}'

        signode += s_nodes.desc_annotation('type', 'type')
        signode += d_nodes.Text(' ')
        signode += s_nodes.desc_name(display, display)
        return fullname

    def add_target_and_index(self, name, sig, signode):
        return super().add_target_and_index(
            f'type::{name}', sig, signode)


class EQLKeywordDirective(BaseEQLDirective):

    def handle_signature(self, sig, signode):
        signode['eql-name'] = sig
        signode['eql-fullname'] = sig

        display = sig.replace('-', ' ')
        signode += s_nodes.desc_annotation('keyword', 'keyword')
        signode += d_nodes.Text(' ')
        signode += s_nodes.desc_name(display, display)

        return sig

    def add_target_and_index(self, name, sig, signode):
        return super().add_target_and_index(
            f'keyword::{name}', sig, signode)


class EQLSynopsisDirective(shared.CodeBlock):

    has_content = True
    optional_arguments = 0
    required_arguments = 0
    option_spec: Dict[str, Any] = {
        'version-lt': d_directives.unchanged_required
    }

    def run(self):
        self.arguments = ['edgeql-synopsis']
        return super().run()


class EQLReactElement(d_rst.Directive):

    has_content = False
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        node = d_nodes.container()
        node['react-element'] = self.arguments[0]
        return [node]


class EQLSectionIntroPage(d_rst.Directive):

    has_content = False
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        node = d_nodes.container()
        node['section-intro-page'] = self.arguments[0]
        return [node]


class EQLStructElement(d_rst.Directive):

    has_content = False
    optional_arguments = 0
    required_arguments = 1

    def run(self):
        fullname = self.arguments[0]
        modname, _, name = fullname.rpartition('.')
        mod = importlib.import_module(modname)
        cls = getattr(mod, name)
        try:
            code = protocol.render(cls)
        except Exception:
            raise RuntimeError(f'could not render {fullname} struct')
        node = d_nodes.literal_block(code, code)
        node['language'] = 'c'
        return [node]


class EQLOperatorDirective(BaseEQLDirective):

    doc_field_types = [
        INDEX_FIELD,

        EQLTypedField(
            'operand',
            label='Operand',
            names=('optype',),
            typerolename='type'),

        EQLTypedField(
            'resulttype',
            label='Result',
            has_arg=False,
            names=('resulttype',),
            typerolename='type'),
    ]

    def handle_signature(self, sig, signode):
        if self.names:
            name = self.names[0]
        else:
            try:
                name, sig = sig.split(':', 1)
            except Exception as ex:
                raise self.error(
                    f':eql:operator signature must match "NAME: SIGNATURE" '
                    f'template'
                ) from ex
            name = name.strip()

        sig = sig.strip()
        if not name or not sig:
            raise self.error(f'invalid :eql:operator: signature')

        signode['eql-name'] = name
        signode['eql-fullname'] = name
        signode['eql-signature'] = sig

        signode += s_nodes.desc_annotation('operator', 'operator')
        signode += d_nodes.Text(' ')
        signode += s_nodes.desc_name(sig, sig)

        return name

    def add_target_and_index(self, name, sig, signode):
        return super().add_target_and_index(
            f'operator::{name}', sig, signode)


class EQLFunctionDirective(BaseEQLDirective):

    doc_field_types = [
        INDEX_FIELD,
    ]

    def handle_signature(self, sig, signode):
        if debug.flags.disable_docs_edgeql_validation:
            signode['eql-fullname'] = fullname = sig.split('(')[0]
            signode['eql-signature'] = sig
            mod, name = fullname.rsplit('::', 1)
            signode['eql-module'] = mod
            signode['eql-name'] = name

            return fullname

        from edb.edgeql import parser as edgeql_parser
        from edb.edgeql.parser import grammar as edgeql_grammar
        from edb.edgeql import ast as ql_ast
        from edb.edgeql import codegen as ql_gen
        from edb.edgeql import qltypes

        try:
            astnode = edgeql_parser.parse(
                edgeql_grammar.tokens.T_STARTBLOCK,
                f'create function {sig} using SQL function "xxx";')[0]
        except Exception as ex:
            raise self.error(
                f'could not parse function signature {sig!r}: '
                f'{ex.__class__.__name__}({ex.args[0]!r})'
            ) from ex

        if (not isinstance(astnode, ql_ast.CreateFunction) or
                not isinstance(astnode.name, ql_ast.ObjectRef)):
            raise self.error(f'EdgeQL parser returned unsupported AST')

        modname = astnode.name.module
        funcname = astnode.name.name
        if not modname:
            raise self.error(
                f'EdgeQL function declaration is missing namespace')

        func_repr = ql_gen.generate_source(astnode)
        m = re.match(r'''(?xs)
            ^
            create\sfunction\s
            (?P<f>.*?)
            \susing\ssql\sfunction
            .*$
        ''', func_repr)
        if not m or not m.group('f'):
            raise self.error(f'could not recreate function signature from AST')
        func_repr = m.group('f')

        signode['eql-module'] = modname
        signode['eql-name'] = funcname
        signode['eql-fullname'] = fullname = f'{modname}::{funcname}'
        signode['eql-signature'] = func_repr

        signode += s_nodes.desc_annotation('function', 'function')
        signode += d_nodes.Text(' ')
        signode += s_nodes.desc_name(fullname, fullname)

        ret_repr = ql_gen.generate_source(astnode.returning)
        if astnode.returning_typemod is qltypes.TypeModifier.SetOfType:
            ret_repr = f'set of {ret_repr}'
        elif astnode.returning_typemod is qltypes.TypeModifier.OptionalType:
            ret_repr = f'optional {ret_repr}'
        signode += s_nodes.desc_returns(ret_repr, ret_repr)

        return fullname

    def add_target_and_index(self, name, sig, signode):
        return super().add_target_and_index(
            f'function::{name}', sig, signode)


class EQLConstraintDirective(BaseEQLDirective):

    doc_field_types = [
        INDEX_FIELD,
    ]

    def handle_signature(self, sig, signode):
        if debug.flags.disable_docs_edgeql_validation:
            signode['eql-fullname'] = fullname = re.split(r'\(| ', sig)[0]
            signode['eql-signature'] = sig
            mod, name = fullname.split('::')
            signode['eql-module'] = mod
            signode['eql-name'] = name

            return fullname

        from edb.edgeql import parser as edgeql_parser
        from edb.edgeql.parser import grammar as edgeql_grammar
        from edb.edgeql import ast as ql_ast
        from edb.edgeql import codegen as ql_gen

        try:
            astnode = edgeql_parser.parse(
                edgeql_grammar.tokens.T_STARTBLOCK,
                f'create abstract constraint {sig};'
            )[0]
        except Exception as ex:
            raise self.error(
                f'could not parse constraint signature {sig!r}') from ex

        if (not isinstance(astnode, ql_ast.CreateConstraint) or
                not isinstance(astnode.name, ql_ast.ObjectRef)):
            raise self.error(f'EdgeQL parser returned unsupported AST')

        modname = astnode.name.module
        constr_name = astnode.name.name
        if not modname:
            raise self.error(
                f'Missing module in EdgeQL constraint declaration')

        constr_repr = ql_gen.generate_source(astnode)

        m = re.match(r'''(?xs)
            ^
            create\sabstract\sconstraint\s
            (?P<f>.*?)(?:\s*on(?P<subj>.*))?
            $
        ''', constr_repr)
        if not m or not m.group('f'):
            raise self.error(
                f'could not recreate constraint signature from AST')
        constr_repr = m.group('f')

        signode['eql-module'] = modname
        signode['eql-name'] = constr_name
        signode['eql-fullname'] = fullname = f'{modname}::{constr_name}'
        signode['eql-signature'] = constr_repr
        subject = m.group('subj')
        if subject:
            subject = subject.strip()[1:-1]
            signode['eql-subjexpr'] = subject
            signode['eql-signature'] += f' on ({subject})'

        signode += s_nodes.desc_annotation('constraint', 'constraint')
        signode += d_nodes.Text(' ')
        signode += s_nodes.desc_name(fullname, fullname)

        return fullname

    def add_target_and_index(self, name, sig, signode):
        return super().add_target_and_index(
            f'constraint::{name}', sig, signode)


class EQLTypeXRef(s_roles.XRefRole):

    @staticmethod
    def filter_target(target):
        new_target = re.sub(r'''(?xi)
            ^ \s*\bSET\s+OF\s+ | \s*\bOPTIONAL\s+
        ''', '', target)

        if '<' in new_target:
            new_target, _ = new_target.split('<', 1)

        return new_target

    def process_link(self, env, refnode, has_explicit_title, title, target):
        new_target = self.filter_target(target)
        if not has_explicit_title:
            title = target.replace('-', ' ')
        return super().process_link(
            env, refnode, has_explicit_title, title, new_target)


class EQLFunctionXRef(s_roles.XRefRole):

    def process_link(self, env, refnode, has_explicit_title, title, target):
        if not has_explicit_title:
            title += '()'
        return super().process_link(
            env, refnode, has_explicit_title, title, target)


class EQLFunctionDescXRef(s_roles.XRefRole):
    pass


class EQLOperatorDescXRef(s_roles.XRefRole):
    pass


class EQLConstraintXRef(s_roles.XRefRole):
    pass


class GitHubLinkRole:

    DEFAULT_REPO = 'edgedb/edgedb'
    BASE_URL = 'https://github.com/'

    # \x00 means the "<" was backslash-escaped
    explicit_title_re = re.compile(r'^(.+?)\s*(?<!\x00)<(.*?)>$', re.DOTALL)

    link_re = re.compile(
        r'''
            (?:
                (?P<repo>(?:[\w\d\-_]+)/(?:[\w\d\-_]+))
                /
            )?
            (?:
                (?:\#(?P<issue>\d+))
                |
                (?P<commit>[A-Fa-f\d]{8,40})
            )
        ''',
        re.X)

    def __call__(
        self, role, rawtext, text, lineno, inliner, options=None, content=None
    ):
        if options is None:
            options = {}
        if content is None:
            content = []

        matched = self.explicit_title_re.match(text)
        if matched:
            has_explicit_title = True
            title = d_utils.unescape(matched.group(1))
            target = d_utils.unescape(matched.group(2))
        else:
            has_explicit_title = False
            title = d_utils.unescape(text)
            target = d_utils.unescape(text)

        matched = self.link_re.match(target)
        if not matched:
            raise shared.EdgeSphinxExtensionError(f'cannot parse {rawtext}')

        repo = matched.group('repo')
        explicit_repo = True
        if not repo:
            repo = self.DEFAULT_REPO
            explicit_repo = False

        issue = matched.group('issue')
        commit = matched.group('commit')
        if issue:
            postfix = f'issues/{issue}'
        elif commit:
            postfix = f'commit/{commit}'
            if not has_explicit_title:
                if explicit_repo:
                    title = f'{repo}/{commit[:8]}'
                else:
                    title = f'{commit[:8]}'
        else:
            raise shared.EdgeSphinxExtensionError(f'cannot parse {rawtext}')

        url = f'{self.BASE_URL}{repo}/{postfix}'

        node = d_nodes.reference(refuri=url, name=title)
        node['eql-github'] = True
        node += d_nodes.Text(title)
        return [node], []


class EdgeQLDomain(s_domains.Domain):

    name = "eql"
    label = "EdgeQL"

    object_types = {
        'function': s_domains.ObjType('function', 'func', 'func-desc'),
        'constraint': s_domains.ObjType('constraint', 'constraint'),
        'type': s_domains.ObjType('type', 'type'),
        'keyword': s_domains.ObjType('keyword', 'kw'),
        'operator': s_domains.ObjType('operator', 'op', 'op-desc'),
        'statement': s_domains.ObjType('statement', 'stmt'),
    }

    _role_to_object_type = {
        role: tn
        for tn, td in object_types.items() for role in td.roles
    }

    directives = {
        'function': EQLFunctionDirective,
        'constraint': EQLConstraintDirective,
        'type': EQLTypeDirective,
        'keyword': EQLKeywordDirective,
        'operator': EQLOperatorDirective,
        'synopsis': EQLSynopsisDirective,
        'struct': EQLStructElement,

        # TODO: Move to edb domain
        'react-element': EQLReactElement,
        'section-intro-page': EQLSectionIntroPage,
    }

    roles = {
        'func': EQLFunctionXRef(),
        'func-desc': EQLFunctionDescXRef(),
        'constraint': EQLConstraintXRef(),
        'type': EQLTypeXRef(),
        'kw': s_roles.XRefRole(),
        'op': s_roles.XRefRole(),
        'op-desc': EQLOperatorDescXRef(),
        'stmt': s_roles.XRefRole(),

        # TODO: Move to edb domain
        'gh': GitHubLinkRole(),
    }

    desc_roles = {
        'func-desc',
        'op-desc',
    }

    initial_data: Dict[str, Dict[str, Any]] = {
        'objects': {}  # fullname -> docname, objtype, description
    }

    def resolve_xref(
        self, env, fromdocname, builder, type, target, node, contnode
    ):

        objects = self.data['objects']
        expected_type = self._role_to_object_type[type]

        target = target.replace(' ', '-')
        if expected_type == 'keyword':
            targets = [f'keyword::{target}']
        elif expected_type == 'operator':
            targets = [f'operator::{target}']
        elif expected_type == 'statement':
            targets = [f'statement::{target}']
        elif expected_type in {'type', 'function', 'constraint'}:
            targets = [f'{expected_type}::{target}']
            if '::' not in target:
                targets.append(f'{expected_type}::std::{target}')
        else:
            targets = [target]

        docname = None
        obj_type = None
        obj_desc = None
        for target in targets:
            try:
                docname, obj_type, obj_desc = objects[target]
            except KeyError:
                continue

        if docname is None:
            if not node.get('eql-auto-link'):
                # if ref was not found, the :eql: xref may be being used
                # outside of the docs, so try resolving ref from intersphinx
                # inventories
                inventories = InventoryAdapter(env)

                for target in targets:
                    obj_type, name = target.split('::', 1)
                    if ':' not in name:
                        continue
                    docset_name, name = name.split(':', 1)

                    docset = inventories.named_inventory.get(docset_name)
                    if docset is None:
                        continue
                    refs = docset.get('eql:' + obj_type)
                    if refs is None:
                        continue
                    ref = refs.get(obj_type + '::' + name)
                    if ref is None:
                        continue

                    newnode = d_nodes.reference(
                        '', '',
                        internal=False, refuri=ref[2],
                    )
                    if node.get('refexplicit'):
                        newnode.append(d_nodes.Text(contnode.astext()))
                    else:
                        title = contnode.astext()
                        newnode.append(
                            contnode.__class__(
                                title[len(docset_name) + 1:],
                                title[len(docset_name) + 1:]
                            )
                        )
                    return newnode

                raise shared.DomainError(
                    f'cannot resolve :eql:{type}: targeting {target!r}')
            else:
                return

        if obj_type != expected_type:
            raise shared.DomainError(
                f'cannot resolve :eql:{type}: targeting {target!r}: '
                f'the type of referred object {expected_type!r} '
                f'does not match the reftype')

        if node['reftype'] in self.desc_roles:
            node = d_nodes.Text(obj_desc)
        else:
            node = s_nodes_utils.make_refnode(
                builder, fromdocname, docname, target, contnode, None)
            node['eql-type'] = obj_type

        return node

    def resolve_any_xref(
        self, env, fromdocname, builder, target, node, contnode
    ):
        # 'myst-parser' resolves all markdown links as :any: xrefs, so return
        # empty list to prevent sphinx trying to resolve these as :eql: refs
        return []

    def clear_doc(self, docname):
        for fullname, (fn, _l, _d) in list(self.data['objects'].items()):
            if fn == docname:
                del self.data['objects'][fullname]

    def merge_domaindata(self, docnames, otherdata):
        for fullname, (fn, objtype, desc) in otherdata['objects'].items():
            if fn in docnames:
                self.data['objects'][fullname] = (fn, objtype, desc)

    def get_objects(self):
        for refname, (docname, type, _desc) in self.data['objects'].items():
            yield (refname, refname, type, docname, refname, 1)

    def get_full_qualified_name(self, node):
        fn = node.get('eql-fullname')
        if not fn:
            raise self.error('no eql-fullname attribute')
        return fn


class StatementTransform(s_transforms.SphinxTransform):

    default_priority = 5  # before ReferencesResolver

    def apply(self):
        for section in self.document.traverse(d_nodes.section):
            xml_str = section.asdom().toxml(encoding="UTF-8")
            parser = lxml.etree.XMLParser(recover=True, encoding="UTF-8")
            x = lxml.etree.parse(io.BytesIO(xml_str), parser)

            fields = set(x.xpath('field_list/field/field_name/text()'))
            title = x.xpath('title/text()')[0]

            page_title = None
            if 'edb-alt-title' in fields:
                page_titles = x.xpath(
                    '''//field_list/field/field_name[text()="edb-alt-title"]
                        /parent::field/field_body/paragraph/text()
                    ''')
                if page_titles:
                    page_title = page_titles[0]

            if page_title:
                if (not section.children or
                        not isinstance(section.children[0], d_nodes.title)):
                    raise shared.EdgeSphinxExtensionError(
                        f'cannot apply :edb-alt-title: field to the {title!r} '
                        f'section')

                section.children[0]['edb-alt-title'] = page_title

            if 'eql-statement' not in fields:
                continue

            nested_statements = x.xpath(
                '//field_list/field/field_name[text()="eql-statement"]')
            if len(nested_statements) > 1:
                raise shared.EdgeSphinxExtensionError(
                    f'section {title!r} has a nested section with '
                    f'a :eql-statement: field set')

            first_para = x.xpath('paragraph[1]/descendant-or-self::*/text()')
            if not len(first_para):
                raise shared.EdgeSphinxExtensionError(
                    f'section {title!r} is marked with an :eql-statement: '
                    f'and is required to have at least one paragraph')
            first_para = ''.join(first_para)
            summary = BaseEQLDirective.strip_ws(first_para)
            if len(summary) > 79:
                raise shared.EdgeSphinxExtensionError(
                    f'section {title!r} is marked with an :eql-statement: '
                    f'and its first paragraph is longer than 79 characters')

            section['eql-statement'] = 'true'
            section['eql-haswith'] = ('true' if 'eql-haswith' in fields
                                      else 'false')
            section['summary'] = summary

            objects = self.env.domaindata['eql']['objects']
            # Make it so that the statement can be referenced by the
            # lower-case version by default.
            target = 'statement::' + title.lower().replace(' ', '-')

            if target in objects:
                raise shared.EdgeSphinxExtensionError(
                    f'duplicate {title!r} statement')

            objects[target] = (self.env.docname, 'statement', summary)

            section['ids'].append(target)


def setup_domain(app):
    # Dummy lexers; the actual highlighting is implemented
    # in the edgedb.com website code.
    app.add_lexer("sdl-diff", pygments.lexers.TextLexer)
    app.add_lexer("edgeql-diff", pygments.lexers.TextLexer)

    app.add_lexer("edgeql", EdgeQLLexer)
    app.add_lexer("edgeql-repl", EdgeQLLexer)
    app.add_lexer("edgeql-runnable", EdgeQLLexer)
    app.add_lexer("edgeql-synopsis", EdgeQLLexer)
    app.add_lexer("edgeql-result", pygments.lexers.special.TextLexer)

    app.add_role(
        'eql:synopsis',
        shared.InlineCodeRole('edgeql-synopsis'))

    app.add_role(
        'eql:code',
        shared.InlineCodeRole('edgeql'))

    app.add_domain(EdgeQLDomain)

    app.add_transform(StatementTransform)


def setup(app):
    setup_domain(app)
