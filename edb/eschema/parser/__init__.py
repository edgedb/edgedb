#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
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


from .parser import EdgeSchemaParser


def parse_fragment(expr):
    parser = EdgeSchemaParser()
    return parser.parse(expr)


def parse(expr, module_aliases=None):
    tree = parse_fragment(expr)

    return tree


def preload():
    EdgeSchemaParser().get_parser_spec()
