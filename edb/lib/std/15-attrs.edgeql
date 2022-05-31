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

## Generic annotations.

CREATE ABSTRACT ANNOTATION std::description;
ALTER ABSTRACT ANNOTATION std::description {
    CREATE ANNOTATION std::description := 'A short documentation string.';
};
CREATE ABSTRACT ANNOTATION std::title {
    CREATE ANNOTATION std::description := 'A human-readable name.';
};
CREATE ABSTRACT ANNOTATION std::deprecated {
    CREATE ANNOTATION std::description :=
        'A marker that an item is deprecated.';
};
CREATE ABSTRACT ANNOTATION std::identifier;
