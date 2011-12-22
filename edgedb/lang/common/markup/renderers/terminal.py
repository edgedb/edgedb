##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import sys
import contextlib

from semantix.utils import term
from semantix.utils.helper import xrepr

from .. import elements
from . import styles


class _Marker(int):
    pass


SMART_BREAK       = _Marker(1)
SMART_LINES_START = _Marker(2)
SMART_LINES_END   = _Marker(3)

INDENT            = _Marker(10)
INDENT_NO_NL      = _Marker(11)
DEDENT            = _Marker(20)
DEDENT_NO_NL      = _Marker(21)

NEW_LINE          = _Marker(30)


class _styled_str(str):
    def __new__(cls, value='', *, style=None):
        obj = str.__new__(cls, value)
        obj.style = style
        return obj

    def __str__(self):
        if self.style is not None and not self.style.empty:
            return self.style._term_prefix + self + self.style._term_postfix
        else:
            return self


class Buffer:
    def __init__(self, *, max_width=None, styled=False, indentation=0, indent_with=' '*4):
        self.data = []
        self.indentation = 0
        self.indent_with = indent_with

        self.max_width = max_width
        self.styled = styled

    def new_line(self, lines=1):
        for _ in range(lines):
            self.data.append(NEW_LINE)

    @contextlib.contextmanager
    def indent(self, auto_new_line=True):
        if auto_new_line:
            self.data.append(INDENT)
            yield
            self.data.append(DEDENT)
        else:
            self.data.append(INDENT_NO_NL)
            yield
            self.data.append(DEDENT_NO_NL)

    @contextlib.contextmanager
    def smart_lines(self):
        self.data.append(SMART_LINES_START)
        yield
        self.data.append(SMART_LINES_END)

    def smart_break(self):
        self.data.append(SMART_BREAK)

    def write(self, s, style=None):
        s = str(s)

        if self.styled and style is not None and not style.empty:
            s = _styled_str(s, style=style)

        self.data.append(s)

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
                if item.__class__ is _Marker:
                    if item == SMART_LINES_START:
                        smlines += 1
                        smlines_max += 1
                    elif item == SMART_LINES_END:
                        smlines -= 1
                        if not smlines:
                            break
                    elif item == SMART_BREAK:
                        _len += 1
                else:
                    _len += len(item)

                if _len > width:
                    return 0

            if _len < width:
                return smlines_max
            else:
                return 0


        for pos, el in enumerate(data):
            if el.__class__ is _Marker:
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
                    if (not smart_mode) and (max_width is not None) and (max_width - offset > 20):
                        smart_mode = does_fit(pos, data, max_width-offset)
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

                else:
                    assert False

            else:
                result.append(str(el))
                offset += len(el)

        return ''.join(result)


class BaseRenderer:
    def __init__(self, *, indent_with=' '*4, max_width=None, styles=None):
        self.renderers_cache = {}
        self.buffer = Buffer(max_width=max_width, styled=styles, indent_with=indent_with)
        self.max_width = max_width
        self.styles = styles

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
                            renderer = getattr(self, '_render_{}'.format(base._markup_name_safe))
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
        str = ' {} '.format(str)

        strlevel = '='
        if level > 1:
            strlevel = '-'

        if self.max_width:
            return '{str:{strlevel}^{width:d}s}'.format(strlevel=strlevel,
                                                        width=self.max_width,
                                                        str=str)
        else:
            return '----{}----'.format(str)

    def _render_unknown(self, element):
        self.buffer.write(xrepr(element, max_len=120), style=self.styles.unknown_markup)

    def _render_Markup(self, element):
        self.buffer.write(xrepr(element, max_len=120), style=self.styles.unknown_markup)

    @classmethod
    def renders(cls, markup, styles=None, max_width=None):
        renderer = cls(max_width=max_width, styles=styles)
        renderer._render(markup)
        return renderer.buffer.flush()


class DocRenderer(BaseRenderer):
    def _render_doc_Text(self, element):
        self.buffer.write(element.text)

    def _render_doc_Section(self, element):
        self.buffer.write(self._render_header(element.title), style=self.styles.header1)
        self.buffer.new_line(2)

        for el in element.body:
            self._render(el)

        self.buffer.new_line()


class LangRenderer(BaseRenderer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ex_depth = 0

    def _render_lang_TreeNode(self, element):
        with self.buffer.smart_lines():
            self.buffer.write(element.name, style=self.styles.tree_node)
            self.buffer.write(' <0x{:x}> ('.format(int(element.id)), style=self.styles.id)

            child_count = len(element.children)
            if child_count:
                with self.buffer.indent():
                    for idx, (childname, child) in enumerate(element.children.items()):
                        self.buffer.write(childname, style=self.styles.attribute)
                        self.buffer.write(' = ')

                        self._render(child)

                        if idx < (child_count - 1):
                            self.buffer.write(',')
                            self.buffer.smart_break()

            self.buffer.write(')', style=self.styles.id)

    def _render_lang_Ref(self, element):
        self.buffer.write('<Ref {!r} 0x{:x}>'.format(element.refname, element.ref),
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

            self.buffer.write(']', style=self.styles.bracket)

    def _render_lang_Dict(self, element):
        with self.buffer.smart_lines():
            self.buffer.write('{', style=self.styles.bracket)

            item_count = len(element.items)
            if item_count:
                with self.buffer.indent():
                    for idx, (key, value) in enumerate(element.items.items()):
                        self.buffer.write(key, style=self.styles.key)
                        self.buffer.write(': ')

                        self._render(value)

                        if idx < (item_count - 1):
                            self.buffer.write(',')
                            self.buffer.smart_break()

            self.buffer.write('}', style=self.styles.bracket)

    def _render_lang_Object(self, element):
        self.buffer.write('<{}.{} at 0x{:x}>'.format(element.class_module,
                                                     element.class_name,
                                                     element.id),
                          style=self.styles.unknown_object)

    def _render_lang_String(self, element):
        self.buffer.write(xrepr(element.str, max_len=120), style=self.styles.literal)

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

            self.buffer.write('File ')
            self.buffer.write(element.filename, style=self.styles.tb_filename)
            self.buffer.write(', line ')
            self.buffer.write(element.lineno, style=self.styles.tb_lineno)
            self.buffer.write(', in ')
            self.buffer.write(element.name, style=self.styles.tb_name)

            with self.buffer.indent(False):
                self.buffer.new_line()

                if element.lines and element.line_numbers:
                    for lineno, line in zip(element.line_numbers, element.lines):
                        if lineno == element.lineno:
                            self.buffer.write('> ', style=self.styles.tb_current_line)
                            self.buffer.write(line.strip() or '???', style=self.styles.tb_code)
                            break
                    else:
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
        self.buffer.write(self._render_header(element.title, level=2), style=self.styles.header2)
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

                self.buffer.write(self._render_header(msg), style=self.styles.header1)
                self.buffer.new_line(2)

            if element.cause:
                self._render(element.cause)

                self.buffer.new_line(2)
                msg = 'The above exception was the direct cause of the following exception'
                self.buffer.write(self._render_header(msg), style=self.styles.header1)
                self.buffer.new_line(2)

            base_excline = '{}.{}: {}'.format(element.class_module, element.class_name, element.msg)
            self.buffer.write('{}. {}'.format(self.ex_depth, base_excline),
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
    def _render_code_Token(self, element):
        self.buffer.write(element.val, style=self.styles.code)

    def _render_code_Comment(self, element):
        self.buffer.write(element.val, style=self.styles.code_comment)

    def _render_code_Decorator(self, element):
        self.buffer.write(element.val, style=self.styles.code_decorator)

    def _render_code_String(self, element):
        self.buffer.write(element.val, style=self.styles.code_string)

    def _render_code_Number(self, element):
        self.buffer.write(element.val, style=self.styles.code_number)

    def _render_code_ClassName(self, element):
        self.buffer.write(element.val, style=self.styles.code_decl_name)

    def _render_code_FunctionName(self, element):
        self.buffer.write(element.val, style=self.styles.code_decl_name)

    def _render_code_Constant(self, element):
        self.buffer.write(element.val, style=self.styles.code_constant)

    def _render_code_Keyword(self, element):
        self.buffer.write(element.val, style=self.styles.code_keyword)

    def _render_code_Punctuation(self, element):
        self.buffer.write(element.val, style=self.styles.code_punctuation)

    def _render_code_Tag(self, element):
        self.buffer.write(element.val, style=self.styles.code_tag)

    def _render_code_Attribute(self, element):
        self.buffer.write(element.val, style=self.styles.code_attribute)

    def _render_code_Code(self, element):
        for token in element.tokens:
            self._render(token)


class Renderer(DocRenderer, LangRenderer, CodeRenderer):
    pass


renders = Renderer.renders


def render(markup, file=None):
    if file is None:
        file = sys.stdout

    fileno = file.fileno()
    max_width = term.size(fileno)[1]

    style_table = None
    if term.use_colors(fileno):
        max_colors = term.max_colors()
        if max_colors > 255:
            style_table = styles.Dark256
        elif max_colors > 6:
            style_table = styles.Dark16

    rendered = renders(markup, styles=style_table, max_width=max_width)
    if not rendered.endswith('\n'):
        rendered += '\n'

    print(rendered, file=file, end='')
