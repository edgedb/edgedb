.. versionadded:: 2.0

.. _ref_eql_ddl_access_policies:

===============
Access Policies
===============

This section describes the DDL commands pertaining to access policies.

Create access policy
====================

:eql-statement:

:ref:`Declare <ref_eql_sdl_access_policies>` a new object access policy.

.. eql:synopsis::
    :version-lt: 3.0

    [ with <with-item> [, ...] ]
    { create | alter } type <TypeName> "{"
      [ ... ]
      create access policy <name>
        [ when (<condition>) ; ]
        { allow | deny } <action> [, <action> ... ; ]
        [ using (<expr>) ; ]
        [ "{"
           [ set errmessage := value ; ]
           [ create annotation annotation-name := value ; ]
          "}" ]
    "}"

    # where <action> is one of
    all
    select
    insert
    delete
    update [{ read | write }]

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    { create | alter } type <TypeName> "{"
      [ ... ]
      create access policy <name>
        [ when (<condition>) ; ]
        { allow | deny } action [, action ... ; ]
        [ using (<expr>) ; ]
        [ "{"
           [ set errmessage := value ; ]
           [ create annotation annotation-name := value ; ]
          "}" ]
    "}"

    # where <action> is one of
    all
    select
    insert
    delete
    update [{ read | write }]


Description
-----------

The combination :eql:synopsis:`{create | alter} type ... create access policy`
defines a new access policy for a given object type.

Parameters
----------

Most sub-commands and options of this command are identical to the
:ref:`SDL access policy declaration <ref_eql_sdl_access_policies_syntax>`.

:eql:synopsis:`<name>`
    The name of the access policy.

:eql:synopsis:`when (<condition>)`
    Specifies which objects this policy applies to. The
    :eql:synopsis:`<condition>` has to be a :eql:type:`bool` expression.

    When omitted, it is assumed that this policy applies to all objects of a
    given type.

:eql:synopsis:`allow`
    Indicates that qualifying objects should allow access under this policy.

:eql:synopsis:`deny`
    Indicates that qualifying objects should *not* allow access under this
    policy. This flavor supersedes any :eql:synopsis:`allow` policy and can
    be used to selectively deny access to a subset of objects that otherwise
    explicitly allows accessing them.

:eql:synopsis:`all`
    Apply the policy to all actions. It is exactly equivalent to listing
    :eql:synopsis:`select`, :eql:synopsis:`insert`, :eql:synopsis:`delete`,
    :eql:synopsis:`update` actions explicitly.

:eql:synopsis:`select`
    Apply the policy to all selection queries. Note that any object that
    cannot be selected, cannot be modified either. This makes
    :eql:synopsis:`select` the most basic "visibility" policy.

:eql:synopsis:`insert`
    Apply the policy to all inserted objects. If a newly inserted object would
    violate this policy, an error is produced instead.

:eql:synopsis:`delete`
    Apply the policy to all objects about to be deleted. If an object does not
    allow access under this kind of policy, it is not going to be considered
    by any :eql:stmt:`delete` command.

    Note that any object that cannot be selected, cannot be modified either.

:eql:synopsis:`update read`
    Apply the policy to all objects selected for an update. If an object does
    not allow access under this kind of policy, it is not visible cannot be
    updated.

    Note that any object that cannot be selected, cannot be modified either.

:eql:synopsis:`update write`
    Apply the policy to all objects at the end of an update. If an updated
    object violates this policy, an error is produced instead.

    Note that any object that cannot be selected, cannot be modified either.

:eql:synopsis:`update`
    This is just a shorthand for :eql:synopsis:`update read` and
    :eql:synopsis:`update write`.

    Note that any object that cannot be selected, cannot be modified either.

:eql:synopsis:`using <expr>`
    Specifies what the policy is with respect to a given eligible (based on
    :eql:synopsis:`when` clause) object. The :eql:synopsis:`<expr>` has to be
    a :eql:type:`bool` expression. The specific meaning of this value also
    depends on whether this policy flavor is :eql:synopsis:`allow` or
    :eql:synopsis:`deny`.

    When omitted, it is assumed that this policy applies to all eligible
    objects of a given type.

The following subcommands are allowed in the ``create access policy`` block:

.. versionadded:: 3.0

    :eql:synopsis:`set errmessage := <value>`
        Set a custom error message of :eql:synopsis:`<value>` that is displayed
        when this access policy prevents a write action.

:eql:synopsis:`create annotation <annotation-name> := <value>`
    Set access policy annotation :eql:synopsis:`<annotation-name>` to
    :eql:synopsis:`<value>`.

    See :eql:stmt:`create annotation` for details.

Alter access policy
====================

:eql-statement:

:ref:`Declare <ref_eql_sdl_access_policies>` a new object access policy.

.. eql:synopsis::
    :version-lt: 3.0

    [ with <with-item> [, ...] ]
    alter type <TypeName> "{"
      [ ... ]
      alter access policy <name> "{"
        [ when (<condition>) ; ]
        [ reset when ; ]
        { allow | deny } <action> [, <action> ... ; ]
        [ using (<expr>) ; ]
        [ reset expression ; ]
        [ create annotation <annotation-name> := <value> ; ]
        [ alter annotation <annotation-name> := <value> ; ]
        [ drop annotation <annotation-name>; ]
      "}"
    "}"

    # where <action> is one of
    all
    select
    insert
    delete
    update [{ read | write }]

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter type <TypeName> "{"
      [ ... ]
      alter access policy <name> "{"
        [ when (<condition>) ; ]
        [ reset when ; ]
        { allow | deny } <action> [, <action> ... ; ]
        [ using (<expr>) ; ]
        [ set errmessage := value ; ]
        [ reset expression ; ]
        [ create annotation <annotation-name> := <value> ; ]
        [ alter annotation <annotation-name> := <value> ; ]
        [ drop annotation <annotation-name>; ]
      "}"
    "}"

    # where <action> is one of
    all
    select
    insert
    delete
    update [{ read | write }]

Description
-----------

The combination :eql:synopsis:`{create | alter} type ... create access policy`
defines a new access policy for a given object type.

Parameters
----------

The parameters describing the action policy are identical to the parameters
used by ``create action policy``. There are a handful of additional
subcommands that are allowed in the ``create access policy`` block:

:eql:synopsis:`reset when`
    Clear the :eql:synopsis:`when (<condition>)` so that the policy applies to
    all objects of a given type. This is equivalent to ``when (true)``.

:eql:synopsis:`reset expression`
    Clear the :eql:synopsis:`using (<condition>)` so that the policy always
    passes. This is equivalent to ``using (true)``.

:eql:synopsis:`alter annotation <annotation-name>;`
    Alter access policy annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`alter annotation` for details.

:eql:synopsis:`drop annotation <annotation-name>;`
    Remove access policy annotation :eql:synopsis:`<annotation-name>`.
    See :eql:stmt:`drop annotation` for details.


All the subcommands allowed in the ``create access policy`` block are also
valid subcommands for ``alter access policy`` block.


Drop access policy
==================

:eql-statement:

Remove an access policy from an object type.

.. eql:synopsis::

    [ with <with-item> [, ...] ]
    alter type <TypeName> "{"
      [ ... ]
      drop access policy <name> ;
    "}"

Description
-----------

The combination :eql:synopsis:`alter type ... drop access policy`
removes the specified access policy from a given object type.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Access policies <ref_datamodel_access_policies>`
  * - :ref:`SDL > Access policies <ref_eql_sdl_access_policies>`
