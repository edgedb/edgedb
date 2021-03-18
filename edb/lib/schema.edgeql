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

CREATE SCALAR TYPE schema::Cardinality
    EXTENDING enum<One, Many>;

CREATE SCALAR TYPE schema::TargetDeleteAction
    EXTENDING enum<Restrict, DeleteSource, Allow, DeferredRestrict>;

CREATE SCALAR TYPE schema::OperatorKind
    EXTENDING enum<Infix, Postfix, Prefix, Ternary>;

CREATE SCALAR TYPE schema::Volatility
    EXTENDING enum<Immutable, Stable, Volatile>;

CREATE SCALAR TYPE schema::ParameterKind
    EXTENDING enum<VariadicParam, NamedOnlyParam, PositionalParam>;

CREATE SCALAR TYPE schema::TypeModifier
    EXTENDING enum<SetOfType, OptionalType, SingletonType>;


# Base type for all schema entities.
CREATE ABSTRACT TYPE schema::Object EXTENDING std::BaseObject {
    CREATE REQUIRED PROPERTY name -> std::str;
    CREATE REQUIRED PROPERTY internal -> std::bool {
        SET default := false;
    };
    CREATE REQUIRED PROPERTY builtin -> std::bool {
        SET default := false;
    };
    CREATE PROPERTY computed_fields -> array<std::str>;
};


CREATE ABSTRACT TYPE schema::SubclassableObject EXTENDING schema::Object {
    CREATE PROPERTY abstract -> std::bool {
        SET default := false;
    };

    # Backwards compatibility.
    CREATE PROPERTY is_abstract := .abstract;

    CREATE PROPERTY final -> std::bool {
        SET default := false;
    };

    # Backwards compatibility.
    CREATE PROPERTY is_final := .final;
};


# Base type for all *types*.
CREATE ABSTRACT TYPE schema::Type EXTENDING schema::SubclassableObject;
CREATE TYPE schema::PseudoType EXTENDING schema::Type;

ALTER TYPE schema::Type {
    CREATE PROPERTY expr -> std::str;
    CREATE PROPERTY from_alias := EXISTS(.expr);
    # Backwards compatibility.
    CREATE PROPERTY is_from_alias := .from_alias;
};


ALTER TYPE std::BaseObject {
    CREATE REQUIRED LINK __type__ -> schema::Type {
        SET readonly := True;
    };
};


CREATE ABSTRACT LINK schema::reference {
    CREATE PROPERTY owned -> std::bool;
    # Backwards compatibility.
    CREATE PROPERTY is_owned := @owned;
};


CREATE ABSTRACT LINK schema::ordered {
    CREATE PROPERTY index -> std::int64;
};


CREATE TYPE schema::Module EXTENDING schema::Object;


CREATE ABSTRACT TYPE schema::CollectionType EXTENDING schema::Type;


CREATE TYPE schema::Array EXTENDING schema::CollectionType {
    CREATE REQUIRED LINK element_type -> schema::Type {
        ON TARGET DELETE DEFERRED RESTRICT;
    };
    CREATE PROPERTY dimensions -> array<std::int16>;
};


CREATE TYPE schema::TupleElement EXTENDING std::BaseObject {
    CREATE REQUIRED LINK type -> schema::Type {
        ON TARGET DELETE DEFERRED RESTRICT;
    };
    CREATE PROPERTY name -> std::str;
};


CREATE TYPE schema::Tuple EXTENDING schema::CollectionType {
    CREATE MULTI LINK element_types EXTENDING schema::ordered
    -> schema::TupleElement {
        CREATE CONSTRAINT std::exclusive;
    }
};


CREATE TYPE schema::Delta EXTENDING schema::Object {
    CREATE MULTI LINK parents -> schema::Delta;
};


CREATE TYPE schema::Annotation EXTENDING schema::Object {
    CREATE PROPERTY inheritable -> std::bool;
};


CREATE ABSTRACT TYPE schema::AnnotationSubject EXTENDING schema::Object {
    CREATE MULTI LINK annotations EXTENDING schema::reference
    -> schema::Annotation {
        CREATE PROPERTY value -> std::str;
	ON TARGET DELETE ALLOW;
    };
};


CREATE ABSTRACT TYPE schema::InheritingObject
EXTENDING schema::SubclassableObject {
    CREATE MULTI LINK bases EXTENDING schema::ordered
        -> schema::InheritingObject;
    CREATE MULTI LINK ancestors EXTENDING schema::ordered
        -> schema::InheritingObject;
    CREATE PROPERTY inherited_fields -> array<std::str>;
};


CREATE TYPE schema::Parameter EXTENDING schema::Object {
    CREATE REQUIRED LINK type -> schema::Type {
        ON TARGET DELETE DEFERRED RESTRICT;
    };
    CREATE REQUIRED PROPERTY typemod -> schema::TypeModifier;
    CREATE REQUIRED PROPERTY kind -> schema::ParameterKind;
    CREATE REQUIRED PROPERTY num -> std::int64;
    CREATE PROPERTY default -> std::str;
};


CREATE ABSTRACT TYPE schema::CallableObject
    EXTENDING schema::AnnotationSubject
{
    CREATE MULTI LINK params -> schema::Parameter {
        ON TARGET DELETE ALLOW;
    };

    CREATE LINK return_type -> schema::Type {
        ON TARGET DELETE DEFERRED RESTRICT;
    };
    CREATE PROPERTY return_typemod -> schema::TypeModifier;
};


CREATE ABSTRACT TYPE schema::VolatilitySubject EXTENDING schema::Object {
    CREATE PROPERTY volatility -> schema::Volatility {
        # NOTE: this default indicates the default value in the python
        # implementation, but is not itself a source of truth
        SET default := 'Volatile';
    };
};


CREATE TYPE schema::Constraint
    EXTENDING schema::CallableObject, schema::InheritingObject
{
    ALTER LINK params {
        CREATE PROPERTY value -> std::str;
    };
    CREATE PROPERTY expr -> std::str;
    CREATE PROPERTY subjectexpr -> std::str;
    CREATE PROPERTY finalexpr -> std::str;
    CREATE PROPERTY errmessage -> std::str;
    CREATE PROPERTY delegated -> std::bool;
};


CREATE ABSTRACT TYPE schema::ConsistencySubject EXTENDING schema::Object {
    CREATE MULTI LINK constraints EXTENDING schema::reference
    -> schema::Constraint {
        CREATE CONSTRAINT std::exclusive;
	ON TARGET DELETE ALLOW;
    };
};


ALTER TYPE schema::Constraint {
    CREATE LINK subject := .<constraints[IS schema::ConsistencySubject];
};


CREATE TYPE schema::Index EXTENDING schema::AnnotationSubject {
    CREATE PROPERTY expr -> std::str;
};


CREATE ABSTRACT TYPE schema::Source EXTENDING schema::Object {
    CREATE MULTI LINK indexes -> schema::Index {
        CREATE CONSTRAINT std::exclusive;
	ON TARGET DELETE ALLOW;
    };
};


CREATE ABSTRACT TYPE schema::Pointer
    EXTENDING
        schema::InheritingObject, schema::ConsistencySubject,
        schema::AnnotationSubject
{
    CREATE PROPERTY cardinality -> schema::Cardinality;
    CREATE PROPERTY required -> std::bool;
    CREATE PROPERTY readonly -> std::bool;
    CREATE PROPERTY default -> std::str;
    CREATE PROPERTY expr -> std::str;
};


ALTER TYPE schema::Source {
    CREATE MULTI LINK pointers EXTENDING schema::reference -> schema::Pointer {
        CREATE CONSTRAINT std::exclusive;
	ON TARGET DELETE ALLOW;
    };
};


CREATE TYPE schema::Alias EXTENDING schema::AnnotationSubject
{
    CREATE REQUIRED PROPERTY expr -> std::str;
    CREATE REQUIRED LINK type -> schema::Type {
        ON TARGET DELETE DEFERRED RESTRICT;
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


ALTER TYPE schema::ObjectType {
    CREATE MULTI LINK union_of -> schema::ObjectType;
    CREATE MULTI LINK intersection_of -> schema::ObjectType;
    CREATE PROPERTY compound_type := (
        EXISTS .union_of OR EXISTS .intersection_of
    );
    # Backwards compatibility.
    CREATE PROPERTY is_compound_type := .compound_type;
};


CREATE TYPE schema::Link EXTENDING schema::Pointer, schema::Source;


CREATE TYPE schema::Property EXTENDING schema::Pointer;


ALTER TYPE schema::Pointer {
    CREATE LINK source -> schema::Source;
    CREATE LINK target -> schema::Type {
        ON TARGET DELETE DEFERRED RESTRICT;
    }
};


ALTER TYPE schema::Link {
    CREATE MULTI LINK properties := .pointers;
    CREATE PROPERTY on_target_delete -> schema::TargetDeleteAction;
};


ALTER TYPE schema::ObjectType {
    CREATE MULTI LINK links := .pointers[IS schema::Link];
    CREATE MULTI LINK properties := .pointers[IS schema::Property];
};


CREATE TYPE schema::Function
    EXTENDING schema::CallableObject, schema::VolatilitySubject
{
    CREATE PROPERTY fallback -> std::bool {
        SET default := false;
    };
};


CREATE TYPE schema::Operator
    EXTENDING schema::CallableObject, schema::VolatilitySubject
{
    CREATE PROPERTY operator_kind -> schema::OperatorKind;
    CREATE PROPERTY abstract -> std::bool {
        SET default := false;
    };
    # Backwards compatibility.
    CREATE PROPERTY is_abstract := .abstract;
};


CREATE TYPE schema::Cast
    EXTENDING schema::AnnotationSubject, schema::VolatilitySubject
{
    CREATE LINK from_type -> schema::Type;
    CREATE LINK to_type -> schema::Type;
    CREATE PROPERTY allow_implicit -> std::bool;
    CREATE PROPERTY allow_assignment -> std::bool;
};


CREATE TYPE schema::Migration
    EXTENDING schema::AnnotationSubject
{
    CREATE MULTI LINK parents -> schema::Migration;
    CREATE REQUIRED PROPERTY script -> str;
    CREATE PROPERTY message -> str;
};


# The package link is added in sys.edgeql
CREATE TYPE schema::Extension EXTENDING schema::AnnotationSubject;
