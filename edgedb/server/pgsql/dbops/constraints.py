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


from .. import common
from . import base


class Constraint(base.DBObject):
    def __init__(self, subject_name, constraint_name=None):
        self._subject_name = subject_name
        self._constraint_name = constraint_name

    def get_type(self):
        return 'CONSTRAINT'

    def get_subject_type(self):
        raise NotImplementedError

    def get_subject_name(self, quote=True):
        if quote:
            return common.qname(*self._subject_name)
        else:
            return self._subject_name

    def get_id(self):
        return '{} ON {} {}'.format(
            self.constraint_name(), self.get_subject_type(),
            self.get_subject_name())

    def constraint_name(self, quote=True):
        if quote and self._constraint_name:
            return common.quote_ident(self._constraint_name)
        else:
            return self._constraint_name

    async def constraint_code(self):
        raise NotImplementedError
