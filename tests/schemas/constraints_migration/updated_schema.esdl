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


scalar type constraint_length extending str {
    constraint max_len_value(16);
    constraint max_len_value(10);
    constraint min_len_value(5);
    constraint min_len_value(8);
}

scalar type constraint_length_2 extending constraint_length {
    constraint min_len_value(9);
}

scalar type constraint_minmax extending str {
    constraint min_value("99900000");
    constraint min_value("99990000");
    constraint max_value("9999999989");
}

scalar type constraint_strvalue extending str {
    constraint expression on (__subject__[-1:] = '9');

    constraint regexp(r"^\d+$");

    constraint expression on (__subject__[0] = '9');

    constraint regexp(r"^\d+9{3,}.*$");
}

scalar type constraint_enum extending str {
   constraint one_of('foo', 'bar');
}

abstract link translated_label {
    property lang -> str;
    property prop1 -> str;
}

abstract link link_with_unique_property {
    property unique_property -> str {
        constraint exclusive;
    }

    property unique_property2 -> str {
        constraint exclusive;
    }
}

abstract link link_with_unique_property_inherited
    extending link_with_unique_property;

abstract link another_link_with_unique_property {
    property unique_property -> str {
        constraint exclusive;
    }
}

abstract link another_link_with_unique_property_inherited
    extending another_link_with_unique_property;

type Label {
    property text -> str;
}

type Object {
    property name -> str {
        constraint exclusive;
        constraint exclusive on (std::str_lower(__subject__));
    }

    property c_length -> constraint_length;
    property c_length_2 -> constraint_length_2;
    property c_length_3 -> constraint_length_2 {
        constraint min_len_value(10);
    }

    property c_minmax -> constraint_minmax;
    property c_strvalue -> constraint_strvalue;
    property c_enum -> constraint_enum;

    link translated_label extending translated_label -> Label {
        constraint exclusive on ((__subject__@source, __subject__@lang));
        constraint exclusive on (__subject__@prop1);
    }
}

type UniqueName {
    property name -> str {
        constraint exclusive;
    }
    property name2 -> str {
        constraint exclusive;
    }

    link link_with_unique_property
        extending link_with_unique_property -> Object;

    link link_with_unique_property_inherited
        extending link_with_unique_property_inherited -> Object;

    link translated_label extending translated_label -> Label {
        constraint exclusive on ((__subject__@source, __subject__@lang));
        constraint exclusive on (__subject__@prop1);
    }
}

type UniqueNameInherited extending UniqueName {
    inherited property name -> str;
}

type UniqueNameGrandchild extending UniqueNameInherited;

type UniqueDescription {
    property description -> str;

    link another_link_with_unique_property
        extending another_link_with_unique_property -> Object;

    link another_link_with_unique_property_inherited
        extending another_link_with_unique_property_inherited -> Object;
}

type UniqueDescriptionInherited extending UniqueDescription;

type UniqueName_2_Renamed {
    property name -> str {
        constraint exclusive;
    }
}

type UniqueName_2_Inherited extending UniqueName_2_Renamed;

type UniqueName_3 extending UniqueName_2_Renamed;

type UniqueName_4 extending UniqueName_2_Inherited;

type MultiConstraint_Renamed {
    property name -> str {
        constraint exclusive;
        constraint exclusive on (str_lower(__subject__));
    }

    property m1 -> str;
}

type ParentUniqueName {
    property name -> str {
        constraint exclusive;
    }
}

type ReceivingParent extending ParentUniqueName {
    inherited property name -> str;
}

type LosingParent {
    property name -> str;
    property lp -> str;
}

type AbstractConstraintParent {
    property name -> str {
        delegated constraint exclusive;
    }
}

type AbstractConstraintParent2 {
    property name -> str {
        delegated constraint exclusive on (str_lower(__subject__));
    }
}

type AbstractConstraintPureChild extending AbstractConstraintParent;

type AbstractConstraintMixedChild extending AbstractConstraintParent {
    inherited property name -> str {
        constraint exclusive on (str_lower(__subject__));
    }
}

type AbstractConstraintPropagated extending AbstractConstraintParent {
    inherited property name -> str {
        delegated constraint exclusive;
    }
}

type AbstractConstraintParent3 {
    property name -> str {
        delegated constraint exclusive;
        delegated constraint exclusive on (str_lower(__subject__));
    }
}

type AbstractConstraintMultipleParentsFlattening
        extending AbstractConstraintParent, AbstractConstraintParent2 {
    property flat -> str;
}

type LosingAbstractConstraintParent {
    property name -> str {
        constraint exclusive;
    }
}

type LosingAbstractConstraintParent2 {
    property name -> str;
}

type BecomingAbstractConstraint {
    property name -> str {
        delegated constraint exclusive;
    }
}

type BecomingAbstractConstraintChild extending BecomingAbstractConstraint;

type BecomingConcreteConstraint {
    property name -> str {
        constraint exclusive;
    }
}

type BecomingConcreteConstraintChild extending BecomingConcreteConstraint;

type AbstractInheritingNonAbstract {
    property name -> str {
        delegated constraint exclusive;
    }
}

type AbstractInheritingNonAbstractChild
    extending AbstractInheritingNonAbstract;
