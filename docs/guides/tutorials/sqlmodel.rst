.. _ref_guide_sqlmodel:

========
SQLModel
========

Gel supports SQL protocol for communicating with the database. This makes it possible to use it with SQLModel by matching a SQLModel schema to the Gel schema. You don't even need to figure out the conversion yourself because we have an automated tool for that.

This tool becomes available when you install ``edgedb`` Python package. Once you have you Gel project setup, you can generate the SQLModel schema with this command:

.. code-block:: bash

  $ gel-orm sqlmodel --mod sqlmschema --out sqlmschema

The ``--mod`` is required and specifies the name of the root Python module that will be generated (the name will be referenced in the generated code). The ``--out`` indicates the output directory (which will be created if it doesn't exist) for the newly generated module.

Even though SQLModel and Gel both view the schema as a bunch of types with fields and interconnections, there are still some differences between what Gel and SQLModel can represent.


Properties
==========

Property types must match the basic Postgres types supported by SQLModel, so avoid any custom types as they will be skipped. Currently we support the following:

* :eql:type:`std::bool`
* :eql:type:`std::str`
* :eql:type:`std::int16`
* :eql:type:`std::int32`
* :eql:type:`std::int64`
* :eql:type:`std::float32`
* :eql:type:`std::float64`
* :eql:type:`std::uuid`
* :eql:type:`std::bytes`
* :eql:type:`cal::local_date`
* :eql:type:`cal::local_time`
* :eql:type:`cal::local_datetime`
* :eql:type:`std::datetime`

Array properties are supported for all of the above types as well.

Multi properties do not have a uniqueness condition that would be true for every row in an exposed SQL table, so they cannot be properly reflected into a SQLModel schema. That means that the schema generator will omit them from the schema. If you needs to reflect multi properties, consider replacing them with a single array property.


Links
=====

Plain single links are reflected as a relationship.

Multi links get represented as a many-to-many relationship with an implicit intermediary table.

Links that have link properties are reflected as intermediary objects with a ``source`` and ``target`` relationships to the end points of the link. The link properties then become the fields of this link object.

All links automatically generate the ``back_populates`` relationships as well. The name of these back-links takes the format of ``_linkname_SourceName``, which mimics the EdgeQL version of backlinks ``.<linkname[is SourceName]`` format.


Modules
=======

Currently only the ``default`` module is supported and other modules will be ignored when generating SQLModel schema.


Connection String
=================

SQLModel requires a Postgres connection string in order to operate with Gel. One way to get that string is by using ``gel instance credentials --insecure-dsn`` and replacing the protocol name with ``postgresql``.


Example
=======

Let's explore how conversion process works using a small example. Consider a project the following schema:

.. code-block:: sdl

  module default {
    type UserGroup {
      required name: str;
      multi link users: User;
    }

    type User {
      required name: str;
    }

    type Post {
      required body: str;
      required link author: User;
    }
  }

This may be part of a system that has users who can belong to a bunch of groups and have the ability to post notes. This gives us some basic relationship types:

* many-to-one for ``Post`` link ``author``
* many-to-many for ``UserGroup`` multi link ``users``

Once the project is initialized and the schema has been applied to the Gel database we can run the conversion command:

.. code-block:: bash

  $ gel-orm sqlmodel --mod projschema --out projschema

The command will produce the following structure:

.. code-block::

  projschema/
  ├─ __init__.py
  ├─ _sqlabase.py
  ├─ _tables.py
  ├─ default.py

Let's go over the contents of the generated files to see how it all works:

.. tabs::

    .. code-tab:: python
        :caption: _sqlabase.py

        #
        # Automatically generated from Gel schema.
        #
        # Do not edit directly as re-generating this file will overwrite any changes.
        #

        from sqlalchemy import orm as orm


        class Base(orm.DeclarativeBase):
            pass

    .. code-tab:: python
        :caption: _tables.py

        #
        # Automatically generated from Gel schema.
        #
        # Do not edit directly as re-generating this file will overwrite any changes.
        #

        import datetime
        import uuid

        import sqlmodel as sm
        import sqlalchemy as sa



        class UserGroup_users_table(sm.SQLModel, table=True):
            __tablename__ = 'UserGroup.users'
            __mapper_args__ = {"confirm_deleted_rows": False}

            source: uuid.UUID = sm.Field(
                foreign_key="UserGroup.id", primary_key=True,
            )
            target: uuid.UUID = sm.Field(
                foreign_key="User.id", primary_key=True,
            )

    .. code-tab:: python
        :caption: default.py

        #
        # Automatically generated from Gel schema.
        #
        # Do not edit directly as re-generating this file will overwrite any changes.
        #

        from ._tables import *


        class Post(sm.SQLModel, table=True):
            __tablename__ = 'Post'
            __mapper_args__ = {"confirm_deleted_rows": False}

            id: uuid.UUID | None = sm.Field(
                default=None,
                primary_key=True,
                sa_column_kwargs=dict(server_default='uuid_generate_v4()'),
            )
            gel_type_id: uuid.UUID | None = sm.Field(
                default=None,
                sa_column=sa.Column('__type__', server_default='PLACEHOLDER'),
            )

            # Properties:
            body: str = sm.Field(nullable=False)

            # Links:
            author_id: uuid.UUID = sm.Field(
                foreign_key="User.id",
                nullable=False,
            )
            author: 'User' = sm.Relationship(
                back_populates='_author_Post',
            )


        class User(sm.SQLModel, table=True):
            __tablename__ = 'User'
            __mapper_args__ = {"confirm_deleted_rows": False}

            id: uuid.UUID | None = sm.Field(
                default=None,
                primary_key=True,
                sa_column_kwargs=dict(server_default='uuid_generate_v4()'),
            )
            gel_type_id: uuid.UUID | None = sm.Field(
                default=None,
                sa_column=sa.Column('__type__', server_default='PLACEHOLDER'),
            )

            # Properties:
            name: str = sm.Field(nullable=False)

            # Back-links:
            _author_Post: list['Post'] = sm.Relationship(
                back_populates='author',
            )
            _users_UserGroup: list['UserGroup'] = sm.Relationship(
                back_populates='users',
                link_model=UserGroup_users_table,
            )


        class UserGroup(sm.SQLModel, table=True):
            __tablename__ = 'UserGroup'
            __mapper_args__ = {"confirm_deleted_rows": False}

            id: uuid.UUID | None = sm.Field(
                default=None,
                primary_key=True,
                sa_column_kwargs=dict(server_default='uuid_generate_v4()'),
            )
            gel_type_id: uuid.UUID | None = sm.Field(
                default=None,
                sa_column=sa.Column('__type__', server_default='PLACEHOLDER'),
            )

            # Properties:
            name: str = sm.Field(nullable=False)

            # Links:
            users: list['User'] = sm.Relationship(
                back_populates='_users_UserGroup',
                link_model=UserGroup_users_table,
            )

The ``_sqlabase.py`` file contains just the ``Base`` class derived from the SQLAlchemy ``DeclarativeBase`` for the reflected model declarations.

The ``_tables.py`` file contains declarations for the models used as intermediate link tables. In our case it's the model used to represent the many-to-many relationship ``users`` between ``UserGroup`` and ``User``. All such intermediate tables will contain ``source`` and ``target`` fields. Both of the fields are part of the ``primary_key`` and they are UUID foreign keys. The name of the table is automatically generated as ``<Type>_<link>_table``.

Finally, the file containing SQLModel models is ``default.py`` (named after the ``default`` Gel module). It contains ``Post``, ``User``, and ``UserGroup`` model declarations.

Let's start with what all models have in common: ``id`` and ``gel_type_id``. They refer to the unique object ``id`` and to the ``__type__.id`` in the Gel schema. These two UUID fields are managed automatically by Gel and should not be directly modified. Effectively they are supposed to be treated as read-only fields.

Properties
----------

The Gel schema declares a few properties: ``name`` for ``User`` and ``UserGroup`` as well as ``body`` for ``Post``. These get reflected as ``str`` fields in the corresponding models. As long as a property has a valid corresponding SQLModel ``Field`` type it will be reflected in this manner.

Links
-----

Let's first look at the ``Post`` declaration in Gel. A ``Post`` has a link ``author`` pointing to a ``User``. So the reflected class ``Post`` has a UUID ``Field`` ``author_id`` which is a foreign key. There is also the corresponding ``author`` ``Relationship``. The target type of ``author`` is annotated to be ``'User'``.

Each reflected ``Relationship`` also automatically declares a back-link via ``back_populates``. The naming format is ``_<link>_<source-Type>``. For the ``author`` link the name of the back-link is ``_author_Post``.

We can look at the ``User`` model and find ``_author_Post`` ``Relationship`` pointing back to ``list['Post']`` and using ``author`` as the ``back_populates`` value.

``User`` model also has a many-to-many relationship with ``UserGroup``. Since in the Gel schema that is represented by the multi link ``users`` that originates on the ``UserGroup`` type, the ``User`` end of this relationship is a back-link and it follows the back-link naming convention. The relationship is ``_users_UserGroup`` and in addition to ``back_populates`` it also declares the other endpoint as ``list['UserGroup']`` and the ``link_model`` ``UserGroup_users_table`` from ``_tables.py`` is used.

Finally, ``UserGroup`` model has the other half of the many-to-many relationship declaration. It has the same name as the Gel schema: ``users``. Otherwise it mirrors the ``Relationship`` declaration for ``_users_UserGroup``.
