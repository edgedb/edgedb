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

scalar type constraint_minmax_2 extending float64 {
    constraint min_ex_value(13);
    constraint max_ex_value(100);
}

scalar type constraint_strvalue extending str {
    constraint expression on (__subject__[-1:] = '9');

    constraint regexp(r"^\d+$");

    constraint expression on (__subject__[0] = '9');

    constraint regexp(r"^\d+9{3,}.*$");
}

# A variant of one_of that uses an array argument instead of
# a variadic.
abstract constraint my_one_of(one_of: array<anytype>) {
    using (contains(one_of, __subject__));
}


scalar type constraint_enum extending str {
   constraint one_of('foo', 'bar');
}

scalar type constraint_enum2 extending str {
   constraint one_of('notfoo', 'notbar');
}

scalar type constraint_my_enum extending str {
   constraint my_one_of(['fuz', 'buz']);
}


abstract link translated_label {
    lang: str;
    prop1: str;
}

abstract link link_with_unique_property {
    property unique_property -> str {
        # TODO: Move the constraint back here once linkprop constraints
        # supported in conflict selects.
        # constraint exclusive;
    }
    constraint exclusive on (@unique_property);
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
    property name -> str;
    property c_length -> constraint_length;
    property c_length_2 -> constraint_length_2;
    property c_length_3 -> constraint_length_2 {
        constraint min_len_value(10);
    }
    property c_one_of -> str {
        constraint one_of('foo', 'bar');
    }

    property c_minmax -> constraint_minmax;
    property c_ex_minmax -> constraint_minmax_2;
    property c_strvalue -> constraint_strvalue;
    property c_enum -> constraint_enum;
    property c_enum2 -> constraint_enum2 {
        default := 'notfoo';
    }
    property c_my_enum -> constraint_my_enum;
}

type ObjCnstr {
    required property first_name -> str;
    required property last_name -> str;
    link label -> Label;
    constraint exclusive on (__subject__.first_name);
    constraint exclusive on (__subject__.label);
}

type UniqueName {
    property name -> str {
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

    multi link translated_labels extending translated_label -> Label {
        constraint exclusive on ((@source, @lang));
        constraint exclusive on (__subject__@prop1);
    }

    link translated_label_tgt extending translated_label -> Label {
        constraint exclusive on ((__subject__@target, __subject__@lang));
    }

    multi link translated_labels_tgt extending translated_label -> Label {
        constraint exclusive on ((@target, @lang));
    }
}

type UniqueNameInherited extending UniqueName {
    overloaded property name -> str;
}

type UniqueDescription {
    property description -> str {
        constraint exclusive;
    }

    link another_link_with_unique_property
        extending another_link_with_unique_property -> Object;

    link another_link_with_unique_property_inherited
        extending another_link_with_unique_property  -> Object;
}

type UniqueDescriptionInherited extending UniqueDescription;


type UniqueName_2 {
    property name -> str {
        constraint exclusive;
    }
}

type UniqueName_2_Inherited extending UniqueName_2;


type UniqueName_3 extending UniqueName_2 {
    overloaded property name -> str {
        constraint exclusive on (str_lower(__subject__));
    }
}

type UniqueName_4 extending UniqueName_2_Inherited;

type MultiConstraint {
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

type ReceivingParent {
    property name -> str;
}

type LosingParent extending ParentUniqueName {
    overloaded property name -> str;
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
    overloaded property name -> str {
        constraint exclusive on (str_lower(__subject__));
    }
}

type AbstractConstraintPropagated extending AbstractConstraintParent {
    overloaded property name -> str {
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

type LosingAbstractConstraintParent extending AbstractConstraintParent;

type LosingAbstractConstraintParent2 extending AbstractConstraintParent;

type BecomingAbstractConstraint {
    property name -> str {
        constraint exclusive;
    }
}

type BecomingAbstractConstraintChild extending BecomingAbstractConstraint;

type BecomingConcreteConstraint {
    property name -> str {
        delegated constraint exclusive;
    }
}

type BecomingConcreteConstraintChild extending BecomingConcreteConstraint;

type PropertyContainer {
    multi property tags -> str {
        constraint exclusive
    }
}
type PropertyContainerChild extending PropertyContainer;

type Pair {
    required property x -> str;
    required property y -> str;
    constraint exclusive on (( .x, .y ));
}

type Indexing {
    required property x -> str;
    required property y -> array<int16>;
    required property z -> json;
    required property u -> bytes;
    constraint exclusive on ((.x[0]));
    constraint exclusive on ((.y[0]));
    constraint exclusive on ((.z[0]));
    constraint exclusive on ((.u[0]));
}

type Slicing {
    required property x -> str;
    required property y -> array<int16>;
    required property z -> json;
    required property u -> bytes;
    constraint exclusive on ((.x[1:3]));
    constraint exclusive on ((.y[1:3]));
    constraint exclusive on ((.z[1:3]));
    constraint exclusive on ((.u[1:3]));
}

scalar type OrderStatus extending enum<open, processing, complete>;

type Order {
    required property status -> OrderStatus;
    constraint exclusive on ((OrderStatus.open in .status));
}
