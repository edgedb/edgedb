.. _ref_guide_sqlalchemy:

==========
SQLAlchemy
==========

|Gel| supports SQL protocol for communicating with the database. This makes it possible to use it with SQLAlchemy by matching a SQLAlchemy schema to the |Gel| schema. You don't even need to figure out the conversion yourself because we have an automated tool for that.

This tool becomes available when you install ``gel`` Python package. Once you have you |Gel| project setup, you can generate the SQLAlchemy schema with this command:

.. code-block:: bash

  $ gel-orm sqlalchemy --mod sqlaschema --out sqlaschema

The ``--mod`` is required and specifies the name of the root Python module that will be generated (the name will be referenced in the generated code). The ``--out`` indicates the output directory (which will be created if it doesn't exist) for the newly generated module.

Even though SQLAlchemy and |Gel| both view the schema as a bunch of types with fields and interconnections, there are still some differences between what |Gel| and SQLAlchemy can represent.


Properties
==========

Property types must match the basic Postgres types supported by SQLAlchemy, so avoid any custom types as they will be skipped. Currently we support the following:

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

Multi properties do not have a uniqueness condition that would be true for every row in an exposed SQL table, so they cannot be properly reflected into a SQLAlchemy schema. That means that the schema generator will omit them from the schema. If you needs to reflect multi properties, consider replacing them with a single array property.


Links
=====

Plain single links are reflected as a relationship.

Multi links get represented as a many-to-many relationship with an implicit intermediary table.

Links that have link properties are reflected as intermediary objects with a ``source`` and ``target`` relationships to the end points of the link. The link properties then become the fields of this link object.

All links automatically generate the ``back_populates`` relationships as well. The name of these back-links takes the format of ``_linkname_SourceName``, which mimics the EdgeQL version of backlinks ``.<linkname[is SourceName]`` format.


Modules
=======

Multiple modules will be mapped onto multiple Postgres schemas and the schema generator will even write them out into separate Python files as well, following whatever nesting structure the modules have.


Connection String
=================

SQLAlchemy requires a Postgres connection string in order to operate with |Gel|. One way to get that string is by using :gelcmd:`instance credentials --insecure-dsn` and replacing the protocol name with ``postgresql``.


Example
=======

Let's explore how conversion process works using a small example. Consider a project the following schema:

.. code-block:: sdl
  :caption: dbschema/default.gel

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

Once the project is initialized and the schema has been applied to the |Gel| database we can run the conversion command:

.. code-block:: bash

  $ gel-orm sqlalchemy --mod projschema --out projschema

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

        from typing import List, Optional

        import sqlalchemy as sa
        from sqlalchemy import orm as orm

        from ._sqlabase import Base


        UserGroup_users_table = sa.Table(
            'UserGroup.users',
            Base.metadata,
            sa.Column("source", sa.ForeignKey("UserGroup.id")),
            sa.Column("target", sa.ForeignKey("User.id")),
            schema='default',
        )

    .. code-tab:: python
        :caption: default.py

        #
        # Automatically generated from Gel schema.
        #
        # Do not edit directly as re-generating this file will overwrite any changes.
        #

        import datetime
        import uuid

        from typing import List, Optional

        import sqlalchemy as sa
        from sqlalchemy import orm as orm

        from ._sqlabase import Base
        from ._tables import *


        class Post(Base):
            __tablename__ = 'Post'
            __mapper_args__ = {"confirm_deleted_rows": False}

            id: orm.Mapped[uuid.UUID] = orm.mapped_column(
                sa.Uuid(),
                primary_key=True,
                server_default='uuid_generate_v4()',
            )
            gel_type_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
                '__type__',
                sa.Uuid(),
                server_default='PLACEHOLDER',
            )

            # Properties:
            body: orm.Mapped[str] = orm.mapped_column(
                sa.String(), nullable=False,
            )

            # Links:
            author_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
                sa.Uuid(), sa.ForeignKey("User.id"), nullable=False,
            )
            author: orm.Mapped['projschema.default.User'] = orm.relationship(
                back_populates='_author_Post',
            )


        class User(Base):
            __tablename__ = 'User'
            __mapper_args__ = {"confirm_deleted_rows": False}

            id: orm.Mapped[uuid.UUID] = orm.mapped_column(
                sa.Uuid(),
                primary_key=True,
                server_default='uuid_generate_v4()',
            )
            gel_type_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
                '__type__',
                sa.Uuid(),
                server_default='PLACEHOLDER',
            )

            # Properties:
            name: orm.Mapped[str] = orm.mapped_column(
                sa.String(), nullable=False,
            )

            # Back-links:
            _author_Post: orm.Mapped[List['projschema.default.Post']] = \
                orm.relationship(back_populates='author')
            _users_UserGroup: orm.Mapped[List['projschema.default.UserGroup']] = \
                orm.relationship(
                    'projschema.default.UserGroup',
                    secondary=UserGroup_users_table,
                    back_populates='users',
                )


        class UserGroup(Base):
            __tablename__ = 'UserGroup'
            __mapper_args__ = {"confirm_deleted_rows": False}

            id: orm.Mapped[uuid.UUID] = orm.mapped_column(
                sa.Uuid(),
                primary_key=True,
                server_default='uuid_generate_v4()',
            )
            gel_type_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
                '__type__',
                sa.Uuid(),
                server_default='PLACEHOLDER',
            )

            # Properties:
            name: orm.Mapped[str] = orm.mapped_column(
                sa.String(), nullable=False,
            )

            # Links:
            users: orm.Mapped[List['projschema.default.User']] = orm.relationship(
                'projschema.default.User',
                secondary=UserGroup_users_table,
                back_populates='_users_UserGroup',
            )

The ``_sqlabase.py`` file contains just the ``Base`` class for the reflected model declarations.

The ``_tables.py`` file contains declarations for intermediate link tables. In our case it's the table used to represent the many-to-many relationship ``users`` between ``UserGroup`` and ``User``. All such intermediate tables will contain ``source`` and ``target`` ``ForeignKey`` columns. The name of the table is automatically generated as ``<Type>_<link>_table``.

Finally, the file containing SQLAlchemy models is ``default.py`` (named after the ``default`` |Gel| module). It contains ``Post``, ``User``, and ``UserGroup`` model declarations.

Let's start with what all models have in common: ``id`` and ``gel_type_id``. They refer to the unique object ``id`` and to the ``__type__.id`` in the |Gel| schema. These two UUID fields are managed automatically by |Gel| and should not be directly modified. Effectively they are supposed to be treated as read-only fields.

Properties
----------

The |Gel| schema declares a few properties: ``name`` for ``User`` and ``UserGroup`` as well as ``body`` for ``Post``. These get reflected as string mapped columns in the corresponding SQLAlchemy models. As long as a property has a valid corresponding SQLAlchemy type it will be reflected in this manner.

Links
-----

Let's first look at the ``Post`` declaration in |Gel|. A ``Post`` has a link ``author`` pointing to a ``User``. So the reflected class ``Post`` has a ``ForeignKey`` ``author_id`` and the corresponding relationship ``author``.

Note that the ``author`` relationship is annotated with ``orm.Mapped['projschema.default.User']``. This annotation uses the value passed as ``--mod`` in order to correctly specify the type for the ``author`` relationship.

Each reflected relationship also automatically declares a back-link via ``back_populates``. The naming format is ``_<link>_<source-Type>``. For the ``author`` link the name of the back-link is ``_author_Post``.

We can look at the ``User`` model and find ``_author_Post`` relationship pointing back to ``'projschema.default.Post'`` and using ``author`` as the ``back_populates`` value.

``User`` model also has a many-to-many relationship with ``UserGroup``. Since in the |Gel| schema that is represented by the multi link ``users`` that originates on the ``UserGroup`` type, the ``User`` end of this relationship is a back-link and it follows the back-link naming convention. The relationship is ``_users_UserGroup`` and in addition to ``back_populates`` it also declares the other endpoint as ``'projschema.default.UserGroup'`` and secondary link table (from ``_tables.py``) being used.

Finally, ``UserGroup`` model has the other half of the many-to-many relationship declaration. It has the same name as the |Gel| schema: ``users``. Otherwise it mirrors the relationship declaration for ``_users_UserGroup``.
