##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.datastructures import Field
from . import base


class DocMarkup(base.Markup, ns='doc'):
    pass


class Section(DocMarkup):
    title = Field(str, coerce=True)
    body = Field(base.MarkupList, coerce=True)


class Text(DocMarkup):
    text = Field(str)
