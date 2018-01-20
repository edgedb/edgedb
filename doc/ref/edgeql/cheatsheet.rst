.. _ref_edgeql_cheatsheet:


Syntax Cheat Sheet
==================

Link, link property and backwards link:

.. code-block:: eql

    SELECT Foo.bar;         # link
    SELECT Foo.bar@spam;    # link property
    SELECT Foo.<baz;        # backwards link


Example Schema
--------------

For the purposes of many examples it is assumed that the module
``example`` contains the following schema:

.. code-block:: eschema

    abstract concept Text:
        # This is an abstract object containing text.
        required link body to str:
            # Maximum length of text is 10000
            # characters.
            constraint maxlength(10000)

    concept User extending std::Named
    # NamedObject is a standard abstract base class,
    # that provides a name link.

        # A few more optional links:
        link first_name to str
        link last_name to str
        link email to str

    concept SystemUser extending std::User
    # a type of user that represents various automatic systems, that
    # might add comments to issues, perhaps based on some automatic
    # escalation system for unresolved higher priority issues

    abstract concept Owned:
        # By default links are optional.
        required link owner to User

    concept Status extending std::Dictionary
    # Dictionary is a NamedObject variant, that enforces
    # name uniqueness across all instances if its subclass.

    concept Priority extending std::Dictionary

    concept LogEntry extending OwnedObject, Text:
        # LogEntry is an OwnedObject and a Text, so it
        # will have all of their links and attributes,
        # in particular, owner and text links.
        required link spent_time to int

    atom issue_num_t extending std::sequence
    # issue_num_t is defined as a concrete sequence type,
    # used to generate sequential issue numbers.

    concept Comment extending Text, Owned:
        required link issue to Issue
        link parent to Comment

    concept Issue extending std::Named, Owned, Text:

        required link number to issue_num_t:
            readonly := true
            # The number values are automatically generated,
            # and are not supposed to be directly writable.

        required link status to Status

        link priority to Priority

        link watchers to User:
            mapping := '**'
            # The watchers link is mapped to User concept in
            # many-to-many relation.  The default mapping is
            # *1 -- many-to-one.

        link time_estimate to int

        link time_spent_log to LogEntry:
            mapping := '1*'
            # 1* -- one-to-many mapping.

        link start_date to datetime:
            default := SELECT datetime::current_datetime()
            # The default value of start_date will be a
            # result of the EdgeQL expression above.

        link due_date to datetime

        link related_to to Issue:
            mapping := '**'

This schema represents the data model for an issue tracker. There
are ``Users``, who can create an ``Issue``, add a ``Comment`` to an
``Issue``, or add a ``LogEntry`` to document work on a particular
``Issue``. ``Issues`` can be related to each other. A ``User`` can
watch any ``Issue``. Every ``Issue`` has a ``Status`` and possibly a
``Priority``.
