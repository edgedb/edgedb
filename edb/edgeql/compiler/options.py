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


"""EdgeQL compiler options."""


from __future__ import annotations
from typing import *

from dataclasses import dataclass, field as dc_field

if TYPE_CHECKING:
    from edb.schema import functions as s_func
    from edb.schema import objects as s_obj
    from edb.schema import name as s_name
    from edb.schema import types as s_types
    from edb.schema import pointers as s_pointers


@dataclass
class GlobalCompilerOptions:
    """Compiler toggles that affect compilation as a whole."""

    #: Whether to allow the expression to be of a generic type.
    allow_generic_type_output: bool = False

    #: Whether to apply various query rewrites, including access policy.
    apply_query_rewrites: bool = True

    #: Whether to apply user-specified access policies
    apply_user_access_policies: bool = True

    #: Whether to allow specifying 'id' explicitly in INSERT
    allow_user_specified_id: bool = False

    #: Enables constant folding optimization (enabled by default).
    constant_folding: bool = True

    #: Force types of all parameters to std::json
    json_parameters: bool = False

    #: Use material types for pointer targets in schema views.
    schema_view_mode: bool = False

    #: True in compile_bootstrap_script().
    bootstrap_mode: bool = False

    #: Whether to track which subexpressions reference each schema object.
    track_schema_ref_exprs: bool = False

    #: If the expression is being processed in the context of a certain
    #: schema object, i.e. a constraint expression, or a pointer default,
    #: this contains the type of the schema object.
    schema_object_context: Optional[Type[s_obj.Object]] = None

    #: When compiling a function body, specifies function parameter
    #: definitions.
    func_params: Optional[s_func.ParameterLikeList] = None

    #: The name that can be used in a "DML is disallowed in ..."
    #: error. When this is not None, any DML should cause an error.
    in_ddl_context_name: Optional[str] = None

    #: Is this a dev instance of the compiler
    devmode: bool = False

    #: Is the compiler running in testmode
    testmode: bool = False


@dataclass
class CompilerOptions(GlobalCompilerOptions):

    #: Module name aliases.
    modaliases: Mapping[Optional[str], str] = dc_field(default_factory=dict)

    #: External symbol table.
    anchors: Mapping[str, Any] = dc_field(default_factory=dict)

    #: The symbol to assume as the prefix for abbreviated paths.
    path_prefix_anchor: Optional[str] = None

    #: Module to put derived schema objects to.
    derived_target_module: Optional[str] = None

    #: The name to use for the top-level type variant.
    result_view_name: Optional[s_name.QualName] = None

    #: If > 0, Inject implicit LIMIT to every SELECT query.
    implicit_limit: int = 0

    #: Include id property in every shape implicitly.
    implicit_id_in_shapes: bool = False

    #: Include __tid__ computable (.__type__.id) in every shape implicitly.
    implicit_tid_in_shapes: bool = False

    #: Include __tname__ computable (.__type__.name) in every shape implicitly.
    implicit_tname_in_shapes: bool = False

    #: A set of schema types and links that should be treated
    #: as singletons in the context of this compilation.
    singletons: Collection[Union[s_types.Type, s_pointers.Pointer]] = (
        frozenset())

    #: Type references that should be remaped to another type.  This
    #: is for dealing with remapping explicit type names in schema
    #: expressions to their subtypes when necessary.
    type_remaps: Dict[s_types.Type, s_types.Type] = (
        dc_field(default_factory=dict))
