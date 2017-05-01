##
# Copyright (c) 2008-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.edgeql import ast as qlast

from . import attributes
from . import constraints
from . import delta as sd
from . import expr
from . import named
from . import nodes
from . import objects as so
from . import sources


class ViewCommandContext(sd.ClassCommandContext,
                         attributes.AttributeSubjectCommandContext,
                         nodes.NodeCommandContext):
    pass


class ViewCommand(constraints.ConsistencySubjectCommand,
                  attributes.AttributeSubjectCommand,
                  nodes.NodeCommand):
    context_class = ViewCommandContext

    @classmethod
    def _get_metaclass(cls):
        return View


class CreateView(ViewCommand, named.CreateNamedClass):
    astnode = qlast.CreateView


class RenameView(ViewCommand, named.RenameNamedClass):
    pass


class AlterView(ViewCommand, named.AlterNamedClass):
    astnode = qlast.AlterView


class DeleteView(ViewCommand, named.DeleteNamedClass):
    astnode = qlast.DropView


class View(sources.Source, nodes.Node, attributes.AttributeSubject):
    _type = 'view'

    expression = so.Field(expr.ExpressionText, default=None,
                          coerce=True, compcoef=0.909)

    result_type = so.Field(so.NodeClass, None, compcoef=0.833)

    delta_driver = sd.DeltaDriver(
        create=CreateView,
        alter=AlterView,
        rename=RenameView,
        delete=DeleteView
    )

    def copy(self):
        result = super().copy()
        result.expression = self.expression
        return result
