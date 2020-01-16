#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations
from typing import *  # NoQA

from . import abc as s_abc
from . import name as sn
from . import objects as so


if TYPE_CHECKING:
    from edb.schema import schema as s_schema


class DerivableObjectBase(s_abc.Object):
    # Override name field comparison coefficient on the
    # presumption that the derived names may be different,
    # but base names may be equal.
    #
    def compare(self,
                other: so.Object,
                *,
                our_schema: s_schema.Schema,
                their_schema: s_schema.Schema,
                context: Optional[so.ComparisonContext] = None) -> float:
        similarity = super().compare(  # type: ignore
            other, our_schema=our_schema,
            their_schema=their_schema, context=context)

        assert isinstance(similarity, float)
        assert isinstance(self, so.Object)

        if self.get_shortname(our_schema) != other.get_shortname(their_schema):
            similarity *= 0.625

        return similarity

    def derive_name(self,
                    schema: s_schema.Schema,
                    source: so.Object,
                    *qualifiers: str,
                    derived_name_base: Optional[str] = None,
                    module: Optional[str] = None
                    ) -> sn.SchemaName:
        if module is None:
            module = source.get_name(schema).module
        source_name = source.get_name(schema)
        qualifiers = (source_name,) + qualifiers

        return derive_name(
            schema,
            *qualifiers,
            module=module,
            parent=self,
            derived_name_base=derived_name_base)

    def generic(self, schema: s_schema.Schema) -> bool:
        assert isinstance(self, so.Object)
        return self.get_shortname(schema) == self.get_name(schema)

    def get_derived_name_base(self, schema: s_schema.Schema) -> str:
        assert isinstance(self, so.Object)
        return self.get_shortname(schema)

    def get_derived_name(self,
                         schema: s_schema.Schema,
                         source: so.Object,
                         *qualifiers: str,
                         mark_derived: bool = False,
                         derived_name_base: Optional[str] = None,
                         module: Optional[str] = None) -> sn.Name:
        return self.derive_name(
            schema, source, *qualifiers,
            derived_name_base=derived_name_base,
            module=module)


class DerivableObject(so.InheritingObjectBase, DerivableObjectBase):

    # Indicates that the object has been declared as
    # explicitly inherited.
    declared_overloaded = so.SchemaField(
        bool,
        default=False, compcoef=None,
        introspectable=False, inheritable=False, ephemeral=True)

    # Whether the object is a result of refdict inheritance
    # merge.
    inherited = so.SchemaField(
        bool,
        default=False, compcoef=None,
        inheritable=False)


def derive_name(
    schema: s_schema.Schema,
    *qualifiers: str,
    module: str,
    parent: Optional[DerivableObjectBase] = None,
    derived_name_base: Optional[str] = None,
) -> sn.Name:
    if derived_name_base is None:
        assert parent is not None
        derived_name_base = parent.get_derived_name_base(schema)

    name = sn.get_specialized_name(derived_name_base, *qualifiers)

    return sn.Name(name=name, module=module)
