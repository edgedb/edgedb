.. versionadded:: 2.0

.. _ref_eql_sdl_access_policies:

===============
Access Policies
===============

This section describes the SDL declarations pertaining to access policies.

Examples
--------

Declare a schema where users can only see their own profiles:

.. code-block:: sdl
    :version-lt: 3.0

    # Declare some global variables to store "current user"
    # information.
    global current_user_id -> uuid;
    global current_user := (
        select User filter .id = global current_user_id
    );

    type User {
        required property name -> str;
    }

    type Profile {
        link owner -> User;

        # Only allow reading to the owner, but also
        # ensure that a user cannot set the "owner" link
        # to anything but themselves.
        access policy owner_only
            allow all using (.owner = global current_user)
            { errmessage := 'Profile may only be accessed by the owner'; }
    }

.. code-block:: sdl

    # Declare some global variables to store "current user"
    # information.
    global current_user_id: uuid;
    global current_user := (
        select User filter .id = global current_user_id
    );

    type User {
        required name: str;
    }

    type Profile {
        owner: User;

        # Only allow reading to the owner, but also
        # ensure that a user cannot set the "owner" link
        # to anything but themselves.
        access policy owner_only
            allow all using (.owner = global current_user);
    }

.. _ref_eql_sdl_access_policies_syntax:

Syntax
------

Define a new access policy corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_access_policies>`.

.. sdl:synopsis::
    :version-lt: 3.0

    # Access policy used inside a type declaration:
    access policy <name>
      [ when (<condition>) ]
      { allow | deny } <action> [, <action> ... ]
      [ using (<expr>) ]
      [ "{"
         [ errmessage := value ; ]
         [ <annotation-declarations> ]
        "}" ] ;

    # where <action> is one of
    all
    select
    insert
    delete
    update [{ read | write }]

.. sdl:synopsis::

    # Access policy used inside a type declaration:
    access policy <name>
      [ when (<condition>) ]
      { allow | deny } <action> [, <action> ... ]
      [ using (<expr>) ]
      [ "{"
         [ errmessage := value ; ]
         [ <annotation-declarations> ]
        "}" ] ;

    # where <action> is one of
    all
    select
    insert
    delete
    update [{ read | write }]

Description
-----------

Access policies are used to implement object-level security and as such they
are defined on object types. In practice the access policies often work
together with :ref:`global variables <ref_eql_ddl_globals>`.

Access policies are an opt-in feature, so once at least one access policy is
defined for a given type, all access not explicitly allowed by that policy
becomes forbidden.

Any sub-type :ref:`extending <ref_datamodel_inheritance>` a base type also
inherits all the access policies of the base type.

The access policy declaration options are as follows:

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

    The expression must be :ref:`Stable <ref_reference_volatility>`.

    When omitted, it is assumed that this policy applies to all eligible
    objects of a given type.

.. versionadded:: 3.0

    :eql:synopsis:`set errmessage := <value>`
        Set a custom error message of :eql:synopsis:`<value>` that is displayed
        when this access policy prevents a write action.

:sdl:synopsis:`<annotation-declarations>`
    Set access policy :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Access policies <ref_datamodel_access_policies>`
  * - :ref:`DDL > Access policies <ref_eql_ddl_access_policies>`
