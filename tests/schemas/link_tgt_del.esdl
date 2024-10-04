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

type Target1 extending Named {
     multi link extra_tgt -> Target1;
};
type Target1Child extending Target1;
type Target2 extending Named;

type Source1 extending Named {
    link tgt1_restrict -> Target1 {
        on target delete restrict;
    }
    link tgt_union_restrict -> Target1 | Target2 {
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
    multi link tgt_union_m2m_del_source -> Target1 | Target2 {
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

    link tgt1_del_target_orphan -> Target1 {
        on source delete delete target if orphan;
    }
    multi link tgt1_m2m_del_target_orphan -> Target1 {
        on source delete delete target if orphan;
    }
    link self_del_target_orphan -> Named {
        on source delete delete target;
    }

}

# Make sure the existence of aliases doen't cause trouble
alias ASource1 := Source1;
alias ATarget1 := Target1;

type Source2 extending Named {
    link src1_del_source -> Source1 {
        on target delete delete source;
    }
    multi link tgt_m2m -> Target1;
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

type SchemaSource extending Named {
    link schema_restrict -> schema::Object {
        on target delete restrict;
    }
    link schema_m_restrict -> schema::Object {
        on target delete restrict;
    }
}
