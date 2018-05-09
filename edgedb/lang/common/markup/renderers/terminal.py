#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2011-present MagicStack Inc. and the EdgeDB authors.
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


import sys
import contextlib

from edgedb.lang.common import term
from edgedb.lang.common.markup.format import xrepr

from .. import elements
from . import styles as styles_module

SMART_BREAK = 1
SMART_LINES_START = 2
SMART_LINES_END = 3
SMART_SPACE = 4

INDENT = 10
INDENT_NO_NL = 11
DEDENT = 20
DEDENT_NO_NL = 21

NEW_LINE = 30

DATA = 100


class Buffer:
    def __init__(
            self, *, max_width=None, styled=False, indentation=0,
            indent_with=' ' * 4):
        self.data = []
        self.indentation = 0
        self.indent_with = indent_with

        self.max_width = max_width
        self.styled = styled

    def new_line(self, lines=1):
        for _ in range(lines):
            self.data.append((NEW_LINE, ))

    @contextlib.contextmanager
    def indent(self, auto_new_line=True):
        if auto_new_line:
            self.data.append((INDENT, ))
            yield
            self.data.append((DEDENT, ))
        else:
            self.data.append((INDENT_NO_NL, ))
            yield
            self.data.append((DEDENT_NO_NL, ))

    def smart_space(self, space=' '):
        self.data.append((SMART_SPACE, space))

    @contextlib.contextmanager
    def smart_lines(self):
        self.data.append((SMART_LINES_START, ))
        yield
        self.data.append((SMART_LINES_END, ))

    def smart_break(self):
        self.data.append((SMART_BREAK, ))

    def write(self, s, style=None):
        st = None
        if self.styled and style is not None and not style.empty:
            st = style

        self.data.append((DATA, str(s), st))

    def flush(self):
        data = self.data
        self.data = None

        indentation = self.indentation
        indent_with = self.indent_with
        indent_with_len = len(indent_with)
        max_width = self.max_width

        result = []
        smart_mode = 0
        offset = 0

        def does_fit(pos, data, width):
            _len = 0
            smlines = 0
            smlines_max = 0

            for item in data[pos:]:
                code = item[0]
                if code == SMART_LINES_START:
                    smlines += 1
                    smlines_max += 1
                elif code == SMART_LINES_END:
                    smlines -= 1
                    if not smlines:
                        break
                elif code == SMART_BREAK:
                    _len += 1
                elif code == DATA:
                    _len += len(item[1])

                if _len > width:
                    return 0

            if _len < width:
                return smlines_max
            else:
                return 0

        for pos, item in enumerate(data):
            el = item[0]

            if el == INDENT:
                indentation += 1
                if not smart_mode:
                    result.append('\n' + indent_with * indentation)
                    offset = indent_with_len * indentation
            elif el == DEDENT:
                indentation -= 1
                if not smart_mode:
                    result.append('\n' + indent_with * indentation)
                    offset = indent_with_len * indentation
            elif el == INDENT_NO_NL:
                indentation += 1
            elif el == DEDENT_NO_NL:
                indentation -= 1
            elif el == NEW_LINE:
                if not smart_mode:
                    result.append('\n' + indent_with * indentation)
                    offset = indent_with_len * indentation
            elif el == SMART_LINES_START:
                if (not smart_mode) and (max_width is not None) and (
                        max_width - offset > 20):
                    smart_mode = does_fit(pos, data, max_width - offset)
            elif el == SMART_LINES_END:
                if smart_mode:
                    smart_mode -= 1
            elif el == SMART_BREAK:
                if smart_mode:
                    result.append(' ')
                    offset += 1
                else:
                    result.append('\n' + indent_with * indentation)
                    offset = indent_with_len * indentation
            elif el == SMART_SPACE:
                if not smart_mode:
                    result.append(item[1])
            elif el == DATA:
                # ``item[1]`` -- text to output, ``item[2]`` -- its style
                #
                if item[2] is None:
                    result.append(item[1])
                else:
                    # If there's a style object - let's apply it
                    #
                    result.append(item[2].apply(item[1]))
                offset += len(item[1])
            else:
                assert False

        return ''.join(result)


class BaseRenderer:
    def __init__(self, *, indent_with=' ' * 4, max_width=None, styles=None):
        self.renderers_cache = {}
        self.buffer = Buffer(
            max_width=max_width, styled=styles, indent_with=indent_with)
        self.max_width = max_width
        self.styles = styles or styles_module.StylesTable()

    def _render(self, markup):
        cls = markup.__class__
        renderer = None

        if not issubclass(cls, elements.base.Markup):
            return self._render_unknown(markup)

        try:
            renderer = self.renderers_cache[cls]
        except KeyError:
            cls_name = markup.__class__._markup_name_safe

            try:
                renderer = getattr(self, '_render_{}'.format(cls_name))
            except AttributeError:
                for base in markup.__class__.__mro__:
                    if issubclass(base, elements.base.Markup):
                        try:
                            renderer = getattr(
                                self,
                                '_render_{}'.format(base._markup_name_safe))
                        except AttributeError:
                            pass
                        else:
                            self.renderers_cache[cls] = renderer
                            break
            else:
                self.renderers_cache[cls] = renderer

        if renderer is None:
            raise Exception('no renderer found for {!r}'.format(markup))

        return renderer(markup)

    def _render_header(self, str, level=1):
        # TODO: Rendering should be moved to Buffer (as only there we're aware
        # of the current indentation and, therefore, ``max_width``).  While
        # here, we should only yield a buffer opcode.

        str = ' {} '.format(str)

        strlevel = '='
        if level > 1:
            strlevel = '-'

        if self.max_width:
            return '{{str:{strlevel}^{width:d}s}}'.format(
                strlevel=strlevel, width=self.max_width).format(str=str)
        else:
            return '----{}----'.format(str)

    def _render_unknown(self, element):
        self.buffer.write(
            xrepr(element, max_len=120), style=self.styles.unknown_markup)

    def _render_Markup(self, element):
        self.buffer.write(
            xrepr(element, max_len=120), style=self.styles.unknown_markup)

    def _render_OverflowBarier(self, element):
        self.buffer.write('<...>', style=self.styles.overflow)

    def _render_SerializationError(self, element):
        self.buffer.write(
            'Exception during serialization to markup: <{}: {}>'.format(
                element.cls, element.text),
            style=self.styles.serialization_error)

    @classmethod
    def renders(cls, markup, styles=None, max_width=None):
        renderer = cls(max_width=max_width, styles=styles)
        renderer._render(markup)
        return renderer.buffer.flush()


class DocRenderer(BaseRenderer):
    def _render_doc_Text(self, element):
        self.buffer.write(element.text)

    def _render_doc_SourceCode(self, element):
        self.buffer.write(element.text)

    def _render_doc_Marker(self, element):
        self.buffer.write(element.text, style=self.styles.marker)
        self.buffer.write(' ')

    def _render_doc_Section(self, element):
        if element.title:
            self.buffer.write(
                self._render_header(element.title), style=self.styles.header1)
            self.buffer.new_line(2)

        for el in element.body:
            self._render(el)
            self.buffer.new_line()

    def _render_doc_ValueDiff(self, element):
        self.buffer.write(element.before, style=self.styles.diff_before)
        self.buffer.write(' | ')
        self.buffer.write(element.after, style=self.styles.diff_after)

    def _render_doc_Diff(self, element):
        total_lines = len(element.lines)
        for linenum, line in enumerate(element.lines):
            style = None
            if line.startswith('+'):
                style = self.styles.diff_after
            elif line.startswith('-'):
                style = self.styles.diff_before
            elif line.startswith('@@'):
                style = self.styles.diff_anno

            self.buffer.write(line, style=style)
            if linenum < total_lines - 1:
                self.buffer.new_line()


class LangRenderer(BaseRenderer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ex_depth = 0

    def _render_lang_TreeNode(self, element):
        with self.buffer.smart_lines():
            self.buffer.write(element.name, style=self.styles.tree_node)

            self.buffer.smart_space()
            if element.id:
                self.buffer.write(
                    '<0x{:x}>'.format(int(element.id)), style=self.styles.id)
                self.buffer.smart_space()
            self.buffer.write('(', style=self.styles.id)

            child_count = len(element.children)
            if child_count:
                key = lambda child: (len(child.label) if child.label else 0)
                longest_lbl = max(element.children, key=key).label
                padding = min(len(longest_lbl) if longest_lbl else 0, 20)

                with self.buffer.indent():
                    for idx, child in enumerate(element.children):
                        if child.label:
                            self.buffer.write(
                                child.label, style=self.styles.attribute)
                            self.buffer.smart_space(
                                ' ' * (max(0, padding - len(child.label)) + 1))
                            self.buffer.write('=')
                            self.buffer.smart_space()

                        self._render(child.node)

                        if idx < (child_count - 1):
                            self.buffer.write(',')
                            self.buffer.smart_break()

            self.buffer.write(')', style=self.styles.id)

    def _render_lang_Ref(self, element):
        self.buffer.write(
            '<Ref {}>'.format(element.refname),
            style=self.styles.ref)

    def _render_lang_List(self, element):
        with self.buffer.smart_lines():
            self.buffer.write('[', style=self.styles.bracket)

            item_count = len(element.items)
            if item_count:
                with self.buffer.indent():
                    for idx, item in enumerate(element.items):
                        self._render(item)

                        if idx < (item_count - 1):
                            self.buffer.write(',')
                            self.buffer.smart_break()

            if element.trimmed:
                self.buffer.write('...')

            self.buffer.write(']', style=self.styles.bracket)

    def _render_mapping_(self, mapping, trimmed=False):
        self.buffer.write('{', style=self.styles.bracket)

        item_count = len(mapping)
        if item_count:
            with self.buffer.indent():
                for idx, (key, value) in enumerate(mapping.items()):
                    self.buffer.write(key, style=self.styles.key)
                    self.buffer.write(': ')

                    self._render(value)

                    if idx < (item_count - 1):
                        self.buffer.write(',')
                        self.buffer.smart_break()

        if trimmed:
            self.buffer.write('...')

        self.buffer.write('}', style=self.styles.bracket)

    def _render_lang_Dict(self, element):
        with self.buffer.smart_lines():
            self._render_mapping_(element.items, trimmed=element.trimmed)

    def _render_lang_Object(self, element):
        if element.attributes or element.repr is None:
            self.buffer.write(
                '<{}.{} at 0x{:x}'.format(
                    element.class_module, element.classname, element.id),
                style=self.styles.unknown_object)

            if element.attributes:
                self.buffer.write(' ')
                self._render_mapping_(element.attributes)

            self.buffer.write('>', style=self.styles.unknown_object)

        else:
            self.buffer.write(element.repr, style=self.styles.unknown_object)

    def _render_lang_String(self, element):
        self.buffer.write(
            xrepr(element.str, max_len=120), style=self.styles.literal)

    def _render_lang_Number(self, element):
        self.buffer.write(element.num, style=self.styles.literal)

    def _render_lang_NoneConstantType(self, element):
        self.buffer.write('None', self.styles.constant)

    def _render_lang_TrueConstantType(self, element):
        self.buffer.write('True', self.styles.constant)

    def _render_lang_FalseConstantType(self, element):
        self.buffer.write('False', self.styles.constant)

    def _render_lang_TracebackPoint(self, element):
        with self.buffer.indent(False):
            self.buffer.new_line()

            self.buffer.write(element.filename, style=self.styles.tb_filename)
            if element.lineno:
                self.buffer.write(', line ')
                self.buffer.write(element.lineno, style=self.styles.tb_lineno)
            if element.address:
                self.buffer.write(', at ')
                self.buffer.write(element.address, style=self.styles.tb_lineno)
            self.buffer.write(', in ')
            self.buffer.write(element.name, style=self.styles.tb_name)

            with self.buffer.indent(False):
                self.buffer.new_line()

                if element.lines and element.line_numbers:
                    for lineno, line in zip(
                            element.line_numbers, element.lines):
                        if lineno == element.lineno:
                            if element.context:
                                stripped_spaces = 0
                                stripped_line = line
                            else:
                                stripped_spaces = len(line) - len(
                                    line.lstrip())
                                stripped_line = line.strip()

                            self.buffer.write(
                                '> ', style=self.styles.tb_current_line)
                            self.buffer.write(
                                stripped_line or '???',
                                style=self.styles.tb_code)

                            if element.colno:
                                # Render column caret
                                _caret_indent = ' ' * (
                                    element.colno - stripped_spaces)
                                self.buffer.new_line()
                                self.buffer.write(' ', style=self.styles.code)
                                self.buffer.write(
                                    _caret_indent + '^',
                                    style=self.styles.tb_pos_caret)
                                self.buffer.new_line()
                            if not element.context:
                                break
                        elif element.context:
                            self.buffer.write('| ', style=self.styles.code)
                            self.buffer.write(
                                line.rstrip(), style=self.styles.code)
                            self.buffer.new_line()
                    else:
                        if not element.context:
                            self.buffer.write('???', style=self.styles.tb_code)

                if element.locals:
                    self.buffer.new_line(2)
                    self.buffer.write('Locals: ', style=self.styles.attribute)
                    self._render(element.locals)
                    self.buffer.new_line()

    def _render_lang_Traceback(self, element):
        for item in element.items:
            self._render(item)

    def _render_lang_ExceptionContext(self, element):
        self.buffer.new_line(2)
        self.buffer.write(
            self._render_header(element.title, level=2),
            style=self.styles.header2)
        self.buffer.new_line()

        if element.body:
            for el in element.body:
                self._render(el)

    def _render_lang_Exception(self, element):
        self.ex_depth += 1
        try:
            if self.ex_depth == 1:
                msg = 'Exception occurred'
                if element.msg:
                    msg = '{}: {}'.format(msg, element.msg)

                self.buffer.write(
                    self._render_header(msg), style=self.styles.header1)
                self.buffer.new_line(2)

            if (element.cause or element.context) is not None:
                if element.cause is None:
                    self._render(element.context)
                    msg = ('During handling of the above exception, '
                           'another exception occurred')
                else:
                    self._render(element.cause)
                    msg = ('The above exception was the direct cause '
                           'of the following exception')

                self.buffer.new_line(2)
                self.buffer.write(
                    self._render_header(msg), style=self.styles.header1)
                self.buffer.new_line(2)

            if element.class_module == 'builtins':
                excclass = element.classname
            else:
                excclass = '{}.{}'.format(
                    element.class_module, element.classname)
            base_excline = '{}: {}'.format(excclass, element.msg)
            self.buffer.write(
                '{}. {}'.format(self.ex_depth, base_excline),
                style=self.styles.exc_title)

            if element.contexts:
                for context in element.contexts:
                    self._render(context)

                self.buffer.new_line()

            self.buffer.new_line()
            self.buffer.write(base_excline, style=self.styles.exc_title)
        finally:
            self.ex_depth -= 1


class CodeRenderer(BaseRenderer):
    def _write_code_token(self, val, style):
        while val.startswith('\n'):
            val = val[1:]
            self.buffer.new_line()

        end_nl = 0
        while val.endswith('\n'):
            val = val[:-1]
            end_nl += 1

        self.buffer.write(val, style=style)

        while end_nl:
            end_nl -= 1
            self.buffer.new_line()

    def _render_code_Token(self, element):
        self._write_code_token(element.val, style=self.styles.code)

    def _render_code_Comment(self, element):
        self._write_code_token(element.val, style=self.styles.code_comment)

    def _render_code_Decorator(self, element):
        self._write_code_token(element.val, style=self.styles.code_decorator)

    def _render_code_String(self, element):
        self._write_code_token(element.val, style=self.styles.code_string)

    def _render_code_Number(self, element):
        self._write_code_token(element.val, style=self.styles.code_number)

    def _render_code_ClassName(self, element):
        self._write_code_token(element.val, style=self.styles.code_decl_name)

    def _render_code_FunctionName(self, element):
        self._write_code_token(element.val, style=self.styles.code_decl_name)

    def _render_code_Constant(self, element):
        self._write_code_token(element.val, style=self.styles.code_constant)

    def _render_code_Keyword(self, element):
        self._write_code_token(element.val, style=self.styles.code_keyword)

    def _render_code_Punctuation(self, element):
        self._write_code_token(element.val, style=self.styles.code_punctuation)

    def _render_code_Tag(self, element):
        self._write_code_token(element.val, style=self.styles.code_tag)

    def _render_code_Attribute(self, element):
        self._write_code_token(element.val, style=self.styles.code_attribute)

    def _render_code_Code(self, element):
        if len(element.tokens) > 20:
            with self.buffer.indent():
                for token in element.tokens:
                    self._render(token)
        else:
            for token in element.tokens:
                self._render(token)


class Renderer(DocRenderer, LangRenderer, CodeRenderer):
    pass


renders = Renderer.renders


def render(markup, *, ensure_newline=True, file=None, renderer=Renderer):
    if file is None:
        file = sys.stdout

    fileno = file.fileno()
    max_width = term.size(fileno)[1]

    style_table = None
    if term.use_colors(fileno):
        max_colors = term.max_colors()
        if max_colors > 255:
            style_table = styles_module.Dark256
        elif max_colors > 6:
            style_table = styles_module.Dark16

    rendered = renderer.renders(
        markup, styles=style_table, max_width=max_width)
    if ensure_newline and not rendered.endswith('\n'):
        rendered += '\n'

    print(rendered, file=file, end='')
