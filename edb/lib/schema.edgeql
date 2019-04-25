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


## INTROSPECTION SCHEMA


CREATE MODULE schema;

CREATE SCALAR TYPE schema::cardinality_t EXTENDING std::str {
    CREATE CONSTRAINT std::one_of ('ONE', 'MANY');
};

CREATE SCALAR TYPE schema::target_delete_action_t EXTENDING std::str {
    CREATE CONSTRAINT std::one_of ('RESTRICT', 'DELETE SOURCE', 'SET EMPTY',
                                   'SET DEFAULT', 'DEFERRED RESTRICT');
};

CREATE SCALAR TYPE schema::operator_kind_t EXTENDING std::str {
    CREATE CONSTRAINT std::one_of ('INFIX', 'POSTFIX', 'PREFIX', 'TERNARY');
};

# Base type for all schema entities.
CREATE ABSTRACT TYPE schema::Object {
    CREATE REQUIRED PROPERTY name -> std::str;
};


# Base type for all *types*.
CREATE ABSTRACT TYPE schema::Type EXTENDING schema::Object;
CREATE TYPE schema::PseudoType EXTENDING schema::Type;


ALTER TYPE std::Object {
    CREATE LINK __type__ -> schema::Type {
        SET readonly := True;
    };
};


CREATE TYPE schema::Module EXTENDING schema::Object;


CREATE ABSTRACT TYPE schema::CollectionType EXTENDING schema::Type;


CREATE TYPE schema::Array EXTENDING schema::CollectionType {
    CREATE REQUIRED LINK element_type -> schema::Type;
    CREATE PROPERTY dimensions -> array<std::int16>;
};


CREATE TYPE schema::TypeElement {
    CREATE REQUIRED LINK type -> schema::Type;
    CREATE REQUIRED PROPERTY num -> std::int16;
    CREATE PROPERTY name -> std::str;
};


CREATE TYPE schema::Tuple EXTENDING schema::CollectionType {
    CREATE MULTI LINK element_types -> schema::TypeElement {
        CREATE CONSTRAINT std::exclusive;
    };
};


CREATE TYPE schema::Delta EXTENDING schema::Object {
    CREATE MULTI LINK parents -> schema::Delta;
};


CREATE TYPE schema::Annotation EXTENDING schema::Object {
    CREATE PROPERTY inheritable -> std::bool;
};


CREATE ABSTRACT TYPE schema::AnnotationSubject EXTENDING schema::Object {
    CREATE MULTI LINK annotations -> schema::Annotation {
        CREATE PROPERTY value -> std::str;
    };
};


CREATE ABSTRACT TYPE schema::InheritingObject EXTENDING schema::Object {
    CREATE MULTI LINK bases -> schema::InheritingObject;
    CREATE MULTI LINK ancestors -> schema::InheritingObject;

    CREATE REQUIRED PROPERTY is_abstract -> std::bool {
        SET default := false;
    };

    CREATE REQUIRED PROPERTY is_final -> std::bool {
        SET default := false;
    };
};


CREATE TYPE schema::Parameter {
    CREATE REQUIRED LINK type -> schema::Type;
    CREATE REQUIRED PROPERTY typemod -> std::str;
    CREATE REQUIRED PROPERTY kind -> std::str;
    CREATE REQUIRED PROPERTY num -> std::int64;
    CREATE PROPERTY name -> std::str;
    CREATE PROPERTY default -> std::str;
};


CREATE ABSTRACT TYPE schema::CallableObject
    EXTENDING schema::AnnotationSubject
{

    CREATE MULTI LINK params -> schema::Parameter {
        CREATE CONSTRAINT std::exclusive;
    };

    CREATE LINK return_type -> schema::Type;
    CREATE PROPERTY return_typemod -> std::str;
};


CREATE TYPE schema::Constraint
    EXTENDING schema::CallableObject, schema::InheritingObject
{
    CREATE MULTI LINK args -> schema::Parameter {
        CREATE CONSTRAINT std::exclusive;
        CREATE PROPERTY value -> std::str;
    };
    CREATE PROPERTY expr -> std::str;
    CREATE PROPERTY subjectexpr -> std::str;
    CREATE PROPERTY finalexpr -> std::str;
    CREATE PROPERTY errmessage -> std::str;
};


CREATE ABSTRACT TYPE schema::ConsistencySubject EXTENDING schema::Object {
    CREATE MULTI LINK constraints -> schema::Constraint {
        CREATE CONSTRAINT std::exclusive;
    };
};


ALTER TYPE schema::Constraint {
    CREATE LINK subject := .<constraints[IS schema::ConsistencySubject];
};


CREATE TYPE schema::Index EXTENDING schema::Object {
    CREATE PROPERTY expr -> std::str;
};


CREATE ABSTRACT TYPE schema::Source EXTENDING schema::Object {
    CREATE MULTI LINK indexes -> schema::Index {
        CREATE CONSTRAINT std::exclusive;
    };
};


CREATE ABSTRACT TYPE schema::Pointer
    EXTENDING
        schema::InheritingObject, schema::ConsistencySubject,
        schema::AnnotationSubject
{
    CREATE REQUIRED PROPERTY cardinality -> schema::cardinality_t;
    CREATE REQUIRED PROPERTY required -> std::bool;
};


ALTER TYPE schema::Source {
    CREATE MULTI LINK pointers -> schema::Pointer {
        CREATE CONSTRAINT std::exclusive;
    };
};


CREATE TYPE schema::ScalarType
    EXTENDING
        schema::InheritingObject, schema::ConsistencySubject,
        schema::AnnotationSubject, schema::Type
{
    CREATE PROPERTY default -> std::str;
    CREATE PROPERTY enum_values -> array<std::str>;
};


CREATE TYPE schema::ObjectType
    EXTENDING
        schema::InheritingObject, schema::ConsistencySubject,
        schema::AnnotationSubject, schema::Type, schema::Source;


CREATE TYPE schema::UnionObjectType
    EXTENDING
        schema::InheritingObject, schema::ConsistencySubject, schema::Type,
        schema::Source;


CREATE TYPE schema::DerivedObjectType
    EXTENDING
        schema::InheritingObject, schema::ConsistencySubject, schema::Type,
        schema::Source;


CREATE TYPE schema::Link EXTENDING schema::Pointer, schema::Source;


CREATE TYPE schema::DerivedLink EXTENDING schema::Pointer, schema::Source;


CREATE TYPE schema::Property EXTENDING schema::Pointer;


ALTER TYPE schema::Pointer {
    CREATE LINK source -> schema::Source;
    CREATE LINK target -> schema::Type;
};


ALTER TYPE schema::Link {
    CREATE LINK properties := .pointers;
    CREATE PROPERTY on_target_delete -> schema::target_delete_action_t;
};


ALTER TYPE schema::ObjectType {
    CREATE LINK links := .pointers[IS schema::Link];
    CREATE LINK properties := .pointers[IS schema::Property];
};


CREATE TYPE schema::Function EXTENDING schema::CallableObject {
    CREATE REQUIRED PROPERTY session_only -> std::bool {
        SET default := false;
    }
};


CREATE TYPE schema::Operator EXTENDING schema::CallableObject {
    CREATE PROPERTY operator_kind -> schema::operator_kind_t;
    CREATE LINK commutator -> schema::Operator;
};


CREATE TYPE schema::Cast EXTENDING schema::Object {
    CREATE LINK from_type -> schema::Type;
    CREATE LINK to_type -> schema::Type;
    CREATE PROPERTY allow_implicit -> std::bool;
    CREATE PROPERTY allow_assignment -> std::bool;
};
