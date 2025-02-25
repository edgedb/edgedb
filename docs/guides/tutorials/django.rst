.. _ref_guide_django:

======
Django
======

|Gel| supports SQL protocol for communicating with the database. This makes it possible to use it with Django by matching a Django schema to the |Gel| schema. You don't even need to figure out the conversion yourself because we have an automated tool for that.

This tool becomes available when you install ``gel`` Python package. Once you have you |Gel| project setup, you can generate the Django schema with this command:

.. code-block:: bash

  $ gel-orm django --out models.py

The ``--out`` indicates the output file for the newly generated Django module. You will also need to include ``'gel.orm.django.gelmodels.apps.GelPGModel'`` into the ``INSTALLED_APPS`` for your Django app.

Even though Django and |Gel| both view the schema as a bunch of types with fields and interconnections, there are still some differences between what |Gel| and Django can represent.


Properties
==========

Property types must match the basic Postgres types supported by Django, so avoid any custom types as they will be skipped. Currently we support the following:

* :eql:type:`std::uuid` - ``UUIDField``
* :eql:type:`std::bigint` - ``DecimalField``
* :eql:type:`std::bool` - ``BooleanField``
* :eql:type:`std::bytes` - ``BinaryField``
* :eql:type:`std::decimal` - ``DecimalField``
* :eql:type:`std::float32` - ``FloatField``
* :eql:type:`std::float64` - ``FloatField``
* :eql:type:`std::int16` - ``SmallIntegerField``
* :eql:type:`std::int32` - ``IntegerField``
* :eql:type:`std::int64` - ``BigIntegerField``
* :eql:type:`std::json` - ``JSONField``
* :eql:type:`std::str` - ``TextField``
* :eql:type:`std::datetime` - ``DateTimeField``
* :eql:type:`cal::local_date` - ``DateField``
* :eql:type:`cal::local_datetime` - ``DateTimeField``
* :eql:type:`cal::local_time` - ``TimeField``

Extreme caution is needed for datetime field, the TZ aware and naive values are controlled in Django via settings (``USE_TZ``) and are mutually exclusive in the same app under default circumstances.

Array properties are supported for all of the above types as well.

Multi properties cannot be represented as they have no primary key at all. If you needs to reflect multi properties, consider replacing them with a single array property.


Links
=====

Plain single links are reflected as a ``ForeignKey``.

Multi links can be represented as link tables in Django schema and used as an implicit intermediary table. Creation and deletion of implicit intermediary table entries works. During creation both ``source`` and ``target`` are specified. While during deletion we rely on |Gel's| machinery to correctly handle deletion based on the target.

Django is quite opinionated about the underlying SQL tables. One such important detail is that it requires a table to have a primary key (PK). Therefore, if a link has link properties we cannot reflect it at all because Django single column PK limits the ability to correctly update the link table.

If you need to include these types of structures, you will need to make them as explicit intermediate objects connected with single links (which by default represent an N-to-1 relationship, so they are multi links in the reverse direction).

Links with link properties can become objects in their own right:

.. code-block:: sdl

  type User {
    name: str;
    # ...
  }

  type UserGroup {
    name: str;
    # ...

    # Replace this kind of link with an explicit object
    # multi link members: User;
  }

  # this would replace a multi link members
  type Members {
    required source: UserGroup;
    required target: User;

    # ... possibly additional payload that used
    # to be link properties
  }

All links automatically generate the ``related_name`` relationships as well. The name of these back-links takes the format of ``_linkname_SourceName``, which mimics the EdgeQL version of backlinks ``.<linkname[is SourceName]`` format.


Modules
=======

Currently multiple modules are not supported for reflection to Django. Only the ``default`` module will be reflected. This limitation comes from a very different way |Gel| and Django view multiple Postgres schemas. Django generally expects there to only be one visible schema and uses multiple schemas as a mechanism to *isolate* data.


Connection String
=================

Django requires a way to connect to Postgres in order to operate with |Gel|. Use :gelcmd:`instance credentials --json` to get the necessary information.


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

  $ gel-orm django --out models.py

The command will produce the following file:

.. code-block:: python

    #
    # Automatically generated from Gel schema.
    #
    # This is based on the auto-generated Django model module, which has been
    # updated to fit Gel schema more closely.
    #

    from django.db import models
    from django.contrib.postgres import fields as pgf


    class GelUUIDField(models.UUIDField):
        # This field must be treated as a auto-generated UUID.
        db_returning = True


    class LTForeignKey(models.ForeignKey):
        # Linked tables need to return their source/target ForeignKeys.
        db_returning = True


    class Post(models.Model):
        id = GelUUIDField(primary_key=True)
        gel_type_id = models.UUIDField(db_column='__type__')

        # properties as Fields
        body = models.TextField()

        # links as ForeignKeys
        author = models.ForeignKey('User', models.DO_NOTHING, related_name='_author_Post')

        class GelPGMeta:
            'This is a model reflected from Gel using Postgres protocol.'

        class Meta:
            managed = False
            db_table = 'Post'


    class User(models.Model):
        id = GelUUIDField(primary_key=True)
        gel_type_id = models.UUIDField(db_column='__type__')

        # properties as Fields
        name = models.TextField()

        class GelPGMeta:
            'This is a model reflected from Gel using Postgres protocol.'

        class Meta:
            managed = False
            db_table = 'User'


    class UserGroup(models.Model):
        id = GelUUIDField(primary_key=True)
        gel_type_id = models.UUIDField(db_column='__type__')

        # properties as Fields
        name = models.TextField()

        # multi links as ManyToManyFields
        users = models.ManyToManyField('User', through='UserGroupUsers', through_fields=("source", "target"), related_name='_users_UserGroup')

        class GelPGMeta:
            'This is a model reflected from Gel using Postgres protocol.'

        class Meta:
            managed = False
            db_table = 'UserGroup'


    class UserGroupUsers(models.Model):

        # links as ForeignKeys
        source = LTForeignKey('UserGroup', models.DO_NOTHING, db_column='source')
        target = LTForeignKey('User', models.DO_NOTHING, db_column='target', primary_key=True)
        class Meta:
            managed = False
            db_table = 'UserGroup.users'
            unique_together = (('source', 'target'),)

The ``GelUUIDField`` class is a custom type used specifically for ``id`` (corresponding to the object ``id`` in |Gel|) and ``gel_type_id`` (corresponding to ``__type__.id`` in |Gel|) which all models have and which are handled entirely by |Gel|. So Django should not attempt to overwrite them.

The ``LTForeignKey`` class is used specifically by the reflected link tables to make sure that the source and target foreign keys are correctly handled.

Next we have the ``Post``, ``User``, and ``UserGroup`` models with their fields and relationships.

Finally, there's the model corresponding to the link table which represents the many-to-many relationship ``users`` between ``UserGroup`` and ``User``. All such intermediate tables will contain ``source`` and ``target`` fields. Only one of them can be a ``primary_key``, even though both are actually important. However, |Gel| takes care of ensuring data integrity, so we can afford to rely on |Gel| to correctly handle deletion cascades when the end-points of links are affected. The name of the table is automatically generated as ``<Type><Link>``.

Properties
----------

The |Gel| schema declares a few properties: ``name`` for ``User`` and ``UserGroup`` as well as ``body`` for ``Post``. These get reflected as ``TextField`` in the corresponding models. As long as a property has a valid corresponding Django ``Field`` type it will be reflected in this manner.

Links
-----

Let's first look at the ``Post`` declaration in |Gel|. A ``Post`` has a link ``author`` pointing to a ``User``. So the reflected type ``Post`` has a ``ForeignKeys`` ``author`` which targets ``'User'``.

Each reflected relationship also automatically declares a back-link via ``related_name``. The naming format is ``_<link>_<source-Type>``. For the ``author`` link the name of the back-link is ``_author_Post``.

The ``User`` model has no links of its own just like in the |Gel| schema.

``UserGroup`` model has a many-to-many relationship with ``User``. The model declares ``users`` as a ``ManyToManyField`` pointing to ``'User'``. The ``through`` relationship is ``UserGroupUsers``. The rules for ``related_name`` are the same as for ``ForeignKey`` and so ``_users_UserGroup`` is declared to be the back-link.

App Settings
------------

In order to use these generated models in your Django app there are a couple of things that need to be added to the settings (typically found in ``settings.py``).

First, we must add ``'gel_pg_models.apps.GelPGModel'`` to ``INSTALLED_APPS``. This will ensure that the |Gel| models are handled correctly, such as making ``id`` and ``gel_type_id`` read-only and managed by |Gel|.

Second, we must configure the ``DATABASES`` to include the connection information for our |Gel| database (using PostgreSQL endpoint).

Running :gelcmd:`instance credentials --json` command produces something like this:

.. code-block:: bash

    $ gel instance credentials --json
    {
      "host": "localhost",
      "port": 10715,
      "user": "admin",
      "password": "h632hKRuss6i9uQeMgEvRsuQ",
      "database": "main",
      "branch": "main",
      "tls_cert_data": "-----BEGIN CERTIFICATE----- <...>",
      "tls_ca": "-----BEGIN CERTIFICATE-----<...>",
      "tls_security": "default"
    }

So we can use that to create the following ``DATABASES`` entry:

.. code-block:: python

    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': 'main',
            'USER': 'admin',
            'PASSWORD': 'h632hKRuss6i9uQeMgEvRsuQ',
            'HOST': 'localhost',
            'PORT': '10715',
        }
    }
