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

CREATE SCALAR TYPE schema::SourceDeleteAction
    EXTENDING enum<DeleteTarget, Allow, DeleteTargetIfOrphan>;

CREATE SCALAR TYPE schema::OperatorKind
    EXTENDING enum<Infix, Postfix, Prefix, Ternary>;

CREATE SCALAR TYPE schema::Volatility
    EXTENDING enum<Immutable, Stable, Volatile, Modifying>;

CREATE SCALAR TYPE schema::ParameterKind
    EXTENDING enum<VariadicParam, NamedOnlyParam, PositionalParam>;

CREATE SCALAR TYPE schema::TypeModifier
    EXTENDING enum<SetOfType, OptionalType, SingletonType>;

CREATE SCALAR TYPE schema::AccessPolicyAction
    EXTENDING enum<Allow, Deny>;

CREATE SCALAR TYPE schema::AccessKind
    EXTENDING enum<`Select`, UpdateRead, UpdateWrite, `Delete`, `Insert`>;

CREATE SCALAR TYPE schema::TriggerTiming
    EXTENDING enum<After, AfterCommitOf>;

CREATE SCALAR TYPE schema::TriggerKind
    EXTENDING enum<`Update`, `Delete`, `Insert`>;

CREATE SCALAR TYPE schema::TriggerScope
    EXTENDING enum<All, Each>;

CREATE SCALAR TYPE schema::RewriteKind
    EXTENDING enum<`Update`, `Insert`>;

CREATE SCALAR TYPE schema::MigrationGeneratedBy
    EXTENDING enum<DevMode, DDLStatement>;

CREATE SCALAR TYPE schema::IndexDeferrability
    EXTENDING enum<Prohibited, Permitted, `Required`>;

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

    CREATE ACCESS POLICY not_internal
        ALLOW SELECT USING (not .internal);
};


CREATE ABSTRACT TYPE schema::SubclassableObject EXTENDING schema::Object {
    CREATE PROPERTY abstract -> std::bool {
        SET default := false;
    };

    # Backwards compatibility.
    CREATE PROPERTY is_abstract := .abstract;

    # Backwards compatibility. (But will maybe become a real property one day.)
    CREATE PROPERTY final := false;

    # Backwards compatibility.
    CREATE PROPERTY is_final := .final;
};


# Base type for all *types*.
CREATE ABSTRACT TYPE schema::Type EXTENDING schema::SubclassableObject;
CREATE TYPE schema::PseudoType EXTENDING schema::Type;

ALTER TYPE schema::Type {
    CREATE PROPERTY expr -> std::str;
    CREATE PROPERTY from_alias -> bool;
    # Backwards compatibility.
    CREATE PROPERTY is_from_alias := .from_alias;
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


CREATE ABSTRACT TYPE schema::PrimitiveType EXTENDING schema::Type;


CREATE ABSTRACT TYPE schema::CollectionType EXTENDING schema::PrimitiveType;


CREATE TYPE schema::Array EXTENDING schema::CollectionType {
    CREATE REQUIRED LINK element_type -> schema::Type;
    CREATE PROPERTY dimensions -> array<std::int16>;
};


CREATE TYPE schema::ArrayExprAlias EXTENDING schema::Array;


CREATE TYPE schema::TupleElement EXTENDING std::BaseObject {
    CREATE REQUIRED LINK type -> schema::Type;
    CREATE PROPERTY name -> std::str;
};


CREATE TYPE schema::Tuple EXTENDING schema::CollectionType {
    CREATE REQUIRED PROPERTY named -> bool;
    CREATE MULTI LINK element_types EXTENDING schema::ordered
    -> schema::TupleElement {
        CREATE CONSTRAINT std::exclusive;
    }
};


CREATE TYPE schema::TupleExprAlias EXTENDING schema::Tuple;


CREATE TYPE schema::Range EXTENDING schema::CollectionType {
    CREATE REQUIRED LINK element_type -> schema::Type;
};


CREATE TYPE schema::RangeExprAlias EXTENDING schema::Range;


CREATE TYPE schema::MultiRange EXTENDING schema::CollectionType {
    CREATE REQUIRED LINK element_type -> schema::Type;
};


CREATE TYPE schema::MultiRangeExprAlias EXTENDING schema::MultiRange;


CREATE TYPE schema::Delta EXTENDING schema::Object {
    CREATE MULTI LINK parents -> schema::Delta;
};


CREATE ABSTRACT TYPE schema::AnnotationSubject EXTENDING schema::Object;

CREATE TYPE schema::Annotation EXTENDING schema::AnnotationSubject {
    CREATE PROPERTY inheritable -> std::bool;
};

ALTER TYPE schema::AnnotationSubject {
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
    CREATE REQUIRED LINK type -> schema::Type;
    CREATE REQUIRED PROPERTY typemod -> schema::TypeModifier;
    CREATE REQUIRED PROPERTY kind -> schema::ParameterKind;
    CREATE REQUIRED PROPERTY num -> std::int64;
    CREATE PROPERTY default -> std::str;
};


CREATE ABSTRACT TYPE schema::CallableObject
    EXTENDING schema::AnnotationSubject
{
    CREATE MULTI LINK params EXTENDING schema::ordered -> schema::Parameter {
        ON TARGET DELETE ALLOW;
    };

    CREATE LINK return_type -> schema::Type;
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
    CREATE PROPERTY except_expr -> std::str;
};


CREATE ABSTRACT TYPE schema::ConsistencySubject
      EXTENDING schema::InheritingObject {
    CREATE MULTI LINK constraints EXTENDING schema::reference
    -> schema::Constraint {
        CREATE CONSTRAINT std::exclusive;
        ON TARGET DELETE ALLOW;
    };
};


ALTER TYPE schema::Constraint {
    CREATE LINK subject -> schema::ConsistencySubject;
};


CREATE TYPE schema::Index
    EXTENDING schema::InheritingObject, schema::AnnotationSubject
{
    CREATE PROPERTY expr -> std::str;
    CREATE PROPERTY except_expr -> std::str;
    CREATE PROPERTY deferrability -> schema::IndexDeferrability;
    CREATE PROPERTY deferred -> std::bool;
    CREATE MULTI LINK params EXTENDING schema::ordered -> schema::Parameter {
        ON TARGET DELETE ALLOW;
    };
    CREATE PROPERTY kwargs -> array<tuple<name: str, expr: str>>;
};


CREATE ABSTRACT TYPE schema::Source EXTENDING schema::Object {
    CREATE MULTI LINK indexes EXTENDING schema::reference -> schema::Index {
        CREATE CONSTRAINT std::exclusive;
        ON TARGET DELETE ALLOW;
    };
};


CREATE ABSTRACT TYPE schema::Pointer
    EXTENDING
        schema::ConsistencySubject,
        schema::AnnotationSubject
{
    CREATE PROPERTY cardinality -> schema::Cardinality;
    CREATE PROPERTY required -> std::bool;
    CREATE PROPERTY readonly -> std::bool;
    CREATE PROPERTY default -> std::str;
    CREATE PROPERTY expr -> std::str;
    CREATE PROPERTY secret -> std::bool;
};


CREATE TYPE schema::AccessPolicy
    EXTENDING
        schema::InheritingObject, schema::AnnotationSubject;


CREATE TYPE schema::Trigger
    EXTENDING
        schema::InheritingObject, schema::AnnotationSubject;


CREATE TYPE schema::Rewrite
    EXTENDING
        schema::InheritingObject, schema::AnnotationSubject;


ALTER TYPE schema::Source {
    CREATE MULTI LINK pointers EXTENDING schema::reference -> schema::Pointer {
        CREATE CONSTRAINT std::exclusive;
        ON TARGET DELETE ALLOW;
    };
};


CREATE TYPE schema::Alias EXTENDING schema::AnnotationSubject
{
    CREATE REQUIRED PROPERTY expr -> std::str;
    # This link is DEFINITELY not optional. This works around
    # compiler weirdness that forces the DEFERRED RESTRICT
    # behavior, which prohibits required-ness.
    CREATE OPTIONAL LINK type -> schema::Type {
        ON TARGET DELETE DEFERRED RESTRICT;
    };
};


CREATE TYPE schema::ScalarType
    EXTENDING
        schema::PrimitiveType,
        schema::ConsistencySubject,
        schema::AnnotationSubject
{
    CREATE PROPERTY default -> std::str;
    CREATE PROPERTY enum_values -> array<std::str>;
    CREATE PROPERTY arg_values -> array<std::str>;
};


CREATE FUNCTION std::sequence_reset(
    seq: schema::ScalarType,
    value: std::int64,
) -> std::int64
{
    SET volatility := 'Volatile';
    USING SQL $$
        SELECT
            pg_catalog.setval(
                pg_catalog.quote_ident(sn.schema)
                    || '.' || pg_catalog.quote_ident(sn.name),
                "value",
                true
            )
        FROM
            ROWS FROM (edgedb_VER.get_user_sequence_backend_name("seq"))
                AS sn(schema text, name text)
    $$;
};


CREATE FUNCTION std::sequence_reset(
    seq: schema::ScalarType,
) -> std::int64
{
    SET volatility := 'Volatile';
    USING SQL $$
        SELECT
            pg_catalog.setval(
                pg_catalog.quote_ident(sn.schema)
                    || '.' || pg_catalog.quote_ident(sn.name),
                s.start_value,
                false
            )
        FROM
            ROWS FROM (edgedb_VER.get_user_sequence_backend_name("seq"))
                AS sn(schema text, name text),
            LATERAL (
                SELECT start_value
                FROM pg_catalog.pg_sequences
                WHERE schemaname = sn.schema AND sequencename = sn.name
            ) AS s
    $$;
};


CREATE FUNCTION std::sequence_next(
    seq: schema::ScalarType,
) -> std::int64
{
    SET volatility := 'Volatile';
    USING SQL $$
        SELECT
            pg_catalog.nextval(
                pg_catalog.quote_ident(sn.schema)
                    || '.' || pg_catalog.quote_ident(sn.name)
            )
        FROM
            ROWS FROM (edgedb_VER.get_user_sequence_backend_name("seq"))
                AS sn(schema text, name text)
    $$;
};


CREATE TYPE schema::ObjectType
    EXTENDING
        schema::Source,
        schema::ConsistencySubject,
        schema::InheritingObject,
        schema::Type,
        schema::AnnotationSubject;


ALTER TYPE std::BaseObject {
    # N.B: Since __type__ is uniquely determined by the type of the
    # source object, as a special-case optimization we do not actually
    # store it in the database. Instead, we inject it into the views
    # we use to implement inheritance and inject it in the compiler
    # when operating on tables directly.
    CREATE REQUIRED LINK __type__ -> schema::ObjectType {
        SET readonly := True;
        SET protected := True;
    };
};


ALTER TYPE schema::ObjectType {
    CREATE MULTI LINK union_of -> schema::ObjectType;
    CREATE MULTI LINK intersection_of -> schema::ObjectType;
    CREATE MULTI LINK access_policies
            EXTENDING schema::reference -> schema::AccessPolicy {
        CREATE CONSTRAINT std::exclusive;
        ON TARGET DELETE ALLOW;
    };
    CREATE MULTI LINK triggers
            EXTENDING schema::reference -> schema::Trigger {
        CREATE CONSTRAINT std::exclusive;
        ON TARGET DELETE ALLOW;
    };
    CREATE PROPERTY compound_type := (
        EXISTS .union_of OR EXISTS .intersection_of
    );
    # Backwards compatibility.
    CREATE PROPERTY is_compound_type := .compound_type;
};


ALTER TYPE schema::AccessPolicy {
  CREATE REQUIRED LINK subject -> schema::ObjectType;
  CREATE MULTI PROPERTY access_kinds -> schema::AccessKind;
  CREATE PROPERTY condition -> std::str;
  CREATE REQUIRED PROPERTY action -> schema::AccessPolicyAction;
  CREATE PROPERTY expr -> std::str;
  CREATE PROPERTY errmessage -> std::str;
};


ALTER TYPE schema::Trigger {
  CREATE REQUIRED LINK subject -> schema::ObjectType;
  CREATE REQUIRED PROPERTY timing -> schema::TriggerTiming;
  CREATE MULTI PROPERTY kinds -> schema::TriggerKind;
  CREATE REQUIRED PROPERTY scope -> schema::TriggerScope;
  CREATE PROPERTY expr -> std::str;
  CREATE PROPERTY condition -> std::str;
};

ALTER TYPE schema::Rewrite {
  CREATE REQUIRED LINK subject -> schema::Pointer;
  CREATE REQUIRED PROPERTY kind -> schema::TriggerKind;
  CREATE REQUIRED PROPERTY expr -> std::str;
};

CREATE TYPE schema::Link EXTENDING schema::Pointer, schema::Source;


CREATE TYPE schema::Property EXTENDING schema::Pointer;


ALTER TYPE schema::Pointer {
    CREATE LINK source -> schema::Source;
    CREATE LINK target -> schema::Type;
    CREATE MULTI LINK rewrites
            EXTENDING schema::reference -> schema::Rewrite {
        CREATE CONSTRAINT std::exclusive;
        ON TARGET DELETE ALLOW;
    };
};


ALTER TYPE schema::Link {
    ALTER LINK target
        SET TYPE schema::ObjectType
        USING (.target[IS schema::ObjectType]);
    CREATE MULTI LINK properties := .pointers[IS schema::Property];
    CREATE PROPERTY on_target_delete -> schema::TargetDeleteAction;
    CREATE PROPERTY on_source_delete -> schema::SourceDeleteAction;
};


ALTER TYPE schema::ObjectType {
    CREATE MULTI LINK links := .pointers[IS schema::Link];
    CREATE MULTI LINK properties := .pointers[IS schema::Property];
};


CREATE TYPE schema::Global EXTENDING schema::AnnotationSubject {
    # This is most definitely NOT optional. It works around some
    # compiler weirdness which requires the on target delete deferred restrict
    CREATE OPTIONAL LINK target -> schema::Type {
        ON TARGET DELETE DEFERRED RESTRICT;
    };
    CREATE PROPERTY required -> std::bool;
    CREATE PROPERTY cardinality -> schema::Cardinality;
    CREATE PROPERTY expr -> std::str;
    CREATE PROPERTY default -> std::str;
};


CREATE TYPE schema::Function
    EXTENDING schema::CallableObject, schema::VolatilitySubject
{
    CREATE PROPERTY preserves_optionality -> std::bool {
        SET default := false;
    };

    CREATE PROPERTY body -> str;
    CREATE REQUIRED PROPERTY language -> str;

    CREATE MULTI LINK used_globals EXTENDING schema::ordered -> schema::Global;
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
    CREATE PROPERTY sdl -> str;
    CREATE PROPERTY message -> str;
    CREATE PROPERTY generated_by -> schema::MigrationGeneratedBy;
};


# The package link is added in sys.edgeql
CREATE TYPE schema::Extension EXTENDING schema::AnnotationSubject;

CREATE TYPE schema::FutureBehavior EXTENDING schema::Object;
