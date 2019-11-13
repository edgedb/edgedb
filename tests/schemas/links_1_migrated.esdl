#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-2016 MagicStack Inc. and the EdgeDB authors.
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


type Target0 {
    property name -> str;
}

type Target1 extending Target0 {
    overloaded property name -> str;
}

type ObjectType0 {
    link target -> Target0;
}

type ObjectType1 {
    link target -> Target0;
}

type ObjectType01 extending ObjectType0;

type ObjectType2 {
    required link target -> Target0;
}

type ObjectType3 {
    required link target -> Target0;
}

type ObjectType23 extending ObjectType2, ObjectType3;
