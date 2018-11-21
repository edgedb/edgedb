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


def get_global_dep_order():
    from . import attributes as s_attr
    from . import constraints as s_constr
    from . import lproperties as s_lprops
    from . import links as s_links
    from . import objtypes as s_objtypes
    from . import scalars as s_scalars

    return (
        s_attr.Attribute,
        s_constr.Constraint,
        s_scalars.ScalarType,
        s_lprops.Property,
        s_links.Link,
        s_objtypes.BaseObjectType,
    )
