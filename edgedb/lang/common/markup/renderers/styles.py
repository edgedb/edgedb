##
# Copyright (c) 2011-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from edgedb.lang.common.term import Style16, Style256


class StylesTable:
    def __getattr__(self, key):
        # If we're querying some non-existing style, pretend it's empty
        #
        return Style16()


class Dark16(StylesTable):
    Style = Style16

    id = Style()
    bracket = Style()

    header1 = Style(color='white')
    header2 = Style(color='black', bold=True)
    exc_title = Style(color='red', bold=True)
    marker = Style(color='red', bold=True)

    tb_name = Style(color='yellow')
    tb_filename = Style()
    tb_lineno = Style()
    tb_current_line = Style()
    tb_code = Style()
    tb_pos_caret = Style(color='white', bold=True)

    attribute = Style(color='black', bold=True)
    key = Style(color='yellow')

    tree_node = Style(color='red', bold=True)

    constant = Style(color='cyan')
    literal = Style(color='green')

    ref = Style(color='red')

    unknown_object = Style(color='blue', bold=True)
    serialization_error = unknown_markup = overflow = Style(
        color='white', bgcolor='red')

    diff_anno = Style(color='white')
    diff_after = Style(color='green')
    diff_before = Style(color='red')

    code = Style(color='white')
    code_decorator = Style(color='black', bold=True)
    code_comment = attribute
    code_string = literal
    code_number = Style(color='green')
    code_constant = constant
    code_punctuation = bracket
    code_keyword = constant
    code_decl_name = tree_node
    code_tag = code_keyword
    code_attribute = Style(color=attribute.color)


class Dark256(StylesTable):
    Style = Style256

    id = Style(color='#3d6559')
    bracket = Style(color='#a7a963')

    header1 = Style(color='#656565')
    header2 = Style(color='#474747')
    exc_title = Style(color='#d84903', bold=True)
    marker = Style(color='#bbb', bgcolor='#582a70', bold=True)

    tb_name = Style(color='#5f5f87', bold=True)
    tb_filename = Style()
    tb_lineno = Style()
    tb_current_line = Style(color='#fff', bold=True)
    tb_code = Style()
    tb_pos_caret = Style(color='white', bold=True)

    attribute = Style(color='#565656', bold=True)
    key = Style(color='#5f875f', bold=True)

    tree_node = Style(color='#bc74d7', bold=True)

    constant = Style(color='#1dbdd0')
    literal = Style(color='#4aa336')

    ref = Style(color='#586c9e')

    unknown_object = Style(color='#454545')
    unknown_markup = overflow = Style(color='white', bgcolor='#84345a')
    serialization_error = Style(color='white', bgcolor='#900')

    diff_anno = Style(color='#777')
    diff_after = Style(color='#4aa336')
    diff_before = Style(color='#A00')

    code = Style(color='#aaa')
    code_decorator = Style(color='#af5f00')
    code_comment = attribute
    code_string = literal
    code_number = Style(color='#af5f5f')
    code_constant = constant
    code_punctuation = bracket
    code_keyword = constant
    code_decl_name = tree_node
    code_tag = code_keyword
    code_attribute = Style(color=attribute.color)
