.. _ref_eql_statements_session_set_alias:

SET
===

:eql-statement:


``SET`` -- set one or multiple session-level parameters

.. eql:synopsis::

    SET MODULE <module> ;
    SET ALIAS <alias> AS MODULE <module> ;


Description
-----------

This command allows altering the configuration of the current session.


Variations
----------

:eql:synopsis:`SET MODULE <module>`
    Set the default module for the current section to *module*.

    For example, if a module ``foo`` contains type ``FooType``,
    the following is how the type can be referred to:

    .. code-block:: edgeql

        # Use the fully-qualified name.
        SELECT foo::FooType;

        # Use the WITH clause to define the default module
        # for the query.
        WITH MODULE foo SELECT foo::FooType;

        # Set the default module for the current session ...
        SET MODULE foo;
        # ... and use an unqualified name.
        SELECT FooType;


:eql:synopsis:`SET ALIAS <alias> AS MODULE <module>`
    Define :eql:synopsis:`<alias>` for the
    :eql:synopsis:`<module>`.

    For example:

    .. code-block:: edgeql

        # Use the fully-qualified name.
        SELECT foo::FooType;

        # Use the WITH clause to define a custom alias
        # for the "foo" module.
        WITH bar AS MODULE foo
        SELECT bar::FooType;

        # Define "bar" as an alias for the "foo" module for
        # the current session ...
        SET ALIAS bar AS MODULE foo;
        # ... and use "bar" instead of "foo".
        SELECT bar::FooType;


Examples
--------

.. code-block:: edgeql

    SET MODULE foo;

    SET ALIAS foo AS MODULE std;


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Reference > EdgeQL > RESET ALIAS
      <ref_eql_statements_session_reset_alias>`
