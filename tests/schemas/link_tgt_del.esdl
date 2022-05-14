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


abstract type Named {
    required property name -> str {
        delegated constraint exclusive;
    }
}

type Target1 extending Named;
type Target1Child extending Target1;

type Source1 extending Named {
    link tgt1_restrict -> Target1 {
        on target delete restrict;
    }
    link tgt1_allow -> Target1 {
        on target delete allow;
    }
    link tgt1_del_source -> Target1 {
        on target delete delete source;
    }
    link tgt1_deferred_restrict -> Target1 {
        on target delete deferred restrict;
    }
    multi link tgt1_m2m_restrict -> Target1 {
        on target delete restrict;
    }
    multi link tgt1_m2m_allow -> Target1 {
        on target delete allow;
    }
    multi link tgt1_m2m_del_source -> Target1 {
        on target delete delete source;
    }

    link tgt1_del_target -> Target1 {
        on source delete delete target;
    }
    multi link tgt1_m2m_del_target -> Target1 {
        on source delete delete target;
    }
    link self_del_target -> Named {
        on source delete delete target;
    }
    link self_del_source -> Named {
        on target delete delete source;
    }
}

type Source2 extending Named {
    link src1_del_source -> Source1 {
        on target delete delete source;
    }
}

type Source3 extending Source1;

type ObjectType4 {
    link foo -> Target1;
}

type ObjectType5 {
    link foo -> Target1;
}

abstract type AbsSource1 extending Named {
    link tgt1_del_source -> Target1 {
        on target delete delete source;
    }
}

type ChildSource1 extending AbsSource1;
