.. _ref_guide_contributing_documentation:

=============
Documentation
=============

:edb-alt-title: Writing EdgeDB Documentation

We pride ourselves on having some of the best documentation around, but we want
you to help us make it even better. Documentation is a great way to get started
contributing to EdgeDB. Improvements to our documentation create a better
experience for every developer coming through the door behind you.

Follow our general and style guidelines to make for a smooth contributing
exprience, both for us and for you. You may notice that the existing
documentation doesn't always follow the guidelines laid out here. They are
aspirational, so there are times when we intentionally break with them. Other
times, bits of documentation may not be touched for a while and so may not
reflect our current guidelines. These are great "low-hanging fruit"
opportunities for your contributions!


Guidelines
==========

- **Avoid changes that don't fix an obvious mistake or add clarity.** This is
  subjective, but try to look at your changes with a critical eye. Do they fix
  errors in the original like misspellings or typos? Do they make existing
  prose more clear or accessible while maintaining accuracy? If you answered
  "yes" to either of those questions, this might be a great addition to our
  docs! If not, consider starting a discussion instead to see if your changes
  might be the exception to this guideline before submitting.
- **Keep commits and pull requests small.** We get it. It's more convenient to
  throw all your changes into a single pull request or even into a single
  commit. The problem is that, if some of the changes are good and others don't
  quite work, having everything in one bucket makes it harder to filter out the
  great changes from those that need more work.
- **Make spelling and grammar fixes in a separate pull request from any content
  changes.** These changes are quick to check and important to anyone reading
  the docs. We want to make sure they hit the live documentation as quickly as
  possible without being bogged down by other changes that require more
  intensive review.

Style
=====

- **Lines should be no longer than 79 characters.** This is enforced by linters
  as part of our CI process. Linting :ref:`can be disabled
  <ref_guide_contributing_documentation_linter_toggle>`, but this should not be
  used unless it's necessary and only for as long as it is necessary.
- **Remove trailing whitespace or whitespace on empty lines.**
- **Surround references to parameter named with asterisks.** You may be tempted
  to surround parameter names with double backticks (````param````). We avoid
  that in favor of ``*param*``, in order to distinguish between parameter
  references and inline code (which *should* be surrounded by double
  backticks).
- **EdgeDB is singular.** Choose "EdgeDB is" over "EdgeDB are" and "EdgeDB
  does" over "EdgeDB do."
- **Use American English spellings.** Choose "color" over "colour" and
  "organize" over "organise."
- **Use the Oxford comma.** When delineating a series, place a comma between
  each item in the series, even the one with the conjunction. Use "eggs, bacon,
  and juice" rather than "eggs, bacon and juice."
- **Write in the simplest prose that is still accurate and expresses everything
  you need to convey.** You may be tempted to write documentation that sounds
  like a computer science textbook. Sometimes that's necessary, but in most
  cases, it isn't. Prioritize accuracy first and accessibility a close second.
- **Be careful using words that have a special meaning in the context of
  EdgeDB.** In casual speech or writing, you might talk about a "set" of
  something in a generic sense. Using the word this way in EdgeDB documentation
  might easily be interpreted as a reference to EdgeDB's :ref:`sets
  <ref_eql_sets>`. Avoid this kind of casual usage of key terms.


Where to Find It
================

Most of our documentation (including this guide) lives in `the edgedb
repository <https://github.com/edgedb/edgedb/>`_ in `the docs directory
<https://github.com/edgedb/edgedb/tree/master/docs>`_.

Documentation for some of our client libraries lives inside the client's repo.
If you don't find it in the edgedb repo at ``docs/clients``, you'll probably
find it alongside the client itself. These clients will also have documentation
stubs inside the edgedb repository directing you to the documentation's
location.

The `EdgeDB tutorial </tutorial>`_ is part of `our web
site repository <https://github.com/edgedb/website>`_. You'll find it in `the
tutorial directory <https://github.com/edgedb/website/tree/main/tutorial>`_.

Finally, our book for beginners titled `Easy EdgeDB </easy-edgedb>`_ lives in
`its own repo <https://github.com/edgedb/easy-edgedb>`_.


How to Build It
===============

edgedb/edgedb
-------------

The ``edgedb`` repository contains all of its documentation in the ``docs/``
directory. Run ``make docs`` to build the documentation in the edgedb repo. The
repository contains a ``Makefile`` for all of Sphinx's necessary build options.
The documentation will be built to ``docs/build``.

To run tests, first :ref:`build EdgeDB locally
<ref_guide_contributing_code_build>`. Then run ``edb test -k doc``.

Building the docs from this repo will not give you a high-fidelity
representation of what users will see on the web site. For that, you may want
to do a full documentation build.

Full Documentation Build
------------------------

A full documentation build requires more setup, but it is the only way to see
your documentation exactly as the user will see it. This is not required, but
it can be useful to help us review and approve your request more quickly by
avoiding mistakes that would be easier to spot when they are fully rendered.

To build, clone `our website repository <https://github.com/edgedb/website>`_
and `follow the installation instructions
<https://github.com/edgedb/website#installation>`_. Then run ``yarn dev`` to
start a development server which also triggers a build of all the
documentation.

.. note::

    The watch task builds documentation changes, but it cannot trigger
    auto-reload in the browser. You will need to manually reload the browser to
    see changes made to the documentation.

Sphinx and reStructuredText
===========================

Our documentation is first built through `Sphinx
<https://www.sphinx-doc.org/>`_ and is written in `reStructuredText
<https://docutils.sourceforge.io/rst.html>`_. If you're unfamiliar with
reStructuredText, `the official primer
<https://docutils.sourceforge.io/docs/user/rst/quickstart.html>`_ is a good
place to start. `The official cheatsheet
<https://docutils.sourceforge.io/docs/user/rst/quickref.html>`_ serves as a
great companion reference while you write. Sphinx also offers their own
`reStructuredText primer
<https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html>`_.

Sphinx not only builds the documentation but also extends reStructuredText to
allow for a more ergonomic experience when crafting docs.

ReStructuredText is an easy-to-learn markup language built for documentation.
Here are the most commonly used elements across our documentation.

reStructuredText Basics
-----------------------

Headings
^^^^^^^^

ReStructuredText headings are underlined (and sometimes overlined) with various
characters. It's flexible about which characters map to which heading levels
and will automatically assign heading levels to characters based on the
hierarchy of the document.

To make it easier to quickly discern the level of a heading across our
documentation, we use a consistent hierarchy across all pages.

1. ``=`` under and over- Used for the top-level heading which is usually the
   page title.
2. ``=`` under only
3. ``-``
4. ``^``

**Example**

.. code-block::

    ==========
    Page Title
    ==========

    Section
    =======

    Sub-Section
    -----------

    Sub-Sub-Section
    ^^^^^^^^^^^^^^^

If you need additional heading levels, you may use the ``.. rubric::``
directive and pass it your heading by adding the heading text on the same line.

**Example**

.. code-block::

    .. rubric:: Yet Another Heading


Inline Formatting
^^^^^^^^^^^^^^^^^

Text can be *italicized* by surrounding it with asterisks.

.. code-block::

    *italicized*

**Bold** text by surrounding it with double asterisks.

.. code-block::

    **Bold**

Labels and Links
^^^^^^^^^^^^^^^^

Labels make it easy to link across our documentation.

.. code-block::

    .. _ref_eql_select_objects:

All pages must have a label at the top, but inner labels are added only when we
need to link to them. Feel free to add a label to a section you need to link
to. Follow the convention of ``_ref_<main-section>_<page>_<section>`` when
naming labels. Check the page's main label at the top if you're not sure how to
name your label. Append an underscore and the name of the section to the page's
label. If you create a page, make sure you add a main label to the top of it.

Create internal links using the ``:ref:`` role. First find the label you want
to link to. Reference the label's name in your role inside backticks (``\```)
removing the leading underscore as in the example below.

**Example**

.. code-block::

    :ref:`ref_eql_select_objects`

**Rendered**

:ref:`ref_eql_select_objects`

The label being linked can be on the same page as the link or on an entirely
different page. Sphinx will find the label and link to the appropriate page and
section.

You may also customize the link text.

**Example**

.. code-block::

    :ref:`our documentation on selecting objects <ref_eql_select_objects>`

**Rendered**

:ref:`our documentation on selecting objects <ref_eql_select_objects>`

To link to documentation for EdgeQL functions, statements, types, operators, or
keywords, see the instructions in
:ref:`ref_guide_contributing_documentation_edgeql`.

Special Paragraphs
^^^^^^^^^^^^^^^^^^

Call out a paragraph as a note or warning using the appropriate directives.

**Example**

.. code-block::

    .. note::

        This paragraph is a note.

**Rendered**

.. note::

    This paragraph is a note.

**Example**

.. code-block::

    .. warning::

        This paragraph is a warning.

**Rendered**

.. warning::

    This paragraph is a warning.

You may also add a title to any of these paragraphs by passing it to the
directive by placing it on the same line.

**Example**

.. code-block::

    .. note:: A Note

        This paragraph is a note.

**Rendered**

.. note:: A Note

    This paragraph is a note.

Reusing Documentation
^^^^^^^^^^^^^^^^^^^^^

If you have documentation that will be reused in multiple contexts, you can
write it in a separate ``.rst`` file and include that file everywhere it should
appear.

.. code-block::

    .. include:: ../stdlib/constraint_table.rst

Tables and Lists
^^^^^^^^^^^^^^^^

We use tables and lists in a few different contexts.

**Example**

.. code-block::

    .. list-table::

        * - Arrays
          - ``array<str>``
        * - Tuples (unnamed)
          - ``tuple<str, int64, bool>``
        * - Tuples (named)
          - ``tuple<name: str, age: int64, is_awesome: bool>``
        * - Ranges
          - ``range<float64>``

**Rendered**

.. list-table::

    * - Arrays
      - ``array<str>``
    * - Tuples (unnamed)
      - ``tuple<str, int64, bool>``
    * - Tuples (named)
      - ``tuple<name: str, age: int64, is_awesome: bool>``
    * - Ranges
      - ``range<float64>``

**Example**

.. code-block::

    .. list-table::
        :class: seealso

        * - **See also**
        * - :ref:`Schema > Access policies <ref_datamodel_access_policies>`
        * - :ref:`SDL > Access policies <ref_eql_sdl_access_policies>`

**Rendered**

.. list-table::
    :class: seealso

    * - **See also**
    * - :ref:`Schema > Access policies <ref_datamodel_access_policies>`
    * - :ref:`SDL > Access policies <ref_eql_sdl_access_policies>`

.. note::

    The ``seealso`` class adds a spacer above the table to push the table
    away from the main page content.

**Example**

.. code-block::

    ====================================== =============================
    Syntax                                 Inferred type
    ====================================== =============================
    :eql:code:`select 3;`                  :eql:type:`int64`
    :eql:code:`select 3.14;`               :eql:type:`float64`
    :eql:code:`select 314e-2;`             :eql:type:`float64`
    :eql:code:`select 42n;`                :eql:type:`bigint`
    :eql:code:`select 42.0n;`              :eql:type:`decimal`
    :eql:code:`select 42e+100n;`           :eql:type:`decimal`
    ====================================== =============================

**Rendered**

====================================== =============================
Syntax                                 Inferred type
====================================== =============================
:eql:code:`select 3;`                  :eql:type:`int64`
:eql:code:`select 3.14;`               :eql:type:`float64`
:eql:code:`select 314e-2;`             :eql:type:`float64`
:eql:code:`select 42n;`                :eql:type:`bigint`
:eql:code:`select 42.0n;`              :eql:type:`decimal`
:eql:code:`select 42e+100n;`           :eql:type:`decimal`
====================================== =============================

Sphinx Basics
-------------

Tables of Contents
^^^^^^^^^^^^^^^^^^

Sphinx requires that every page in the documentation be referenced from a table
of contents. Use the ``.. toctree::`` directive to create a table of contents.

**Example**

.. code-block::

    .. toctree::
        :maxdepth: 3
        :hidden:

        code
        documentation

Most of our tables of contents use the roles you see in this example to set a
maximum depth of 3 and to hide the table of contents. This is not required
though if other options make sense in your context. Even though the tables are
hidden, their content still gets rendered in the left sidebar navigation.

We generally use relative references in the ``toctree`` directive which
reference the pages relative to the location of the page that contains the
directive. The order of the references in the directive determines their order
in the sidebar navigation.

If any document is not included in any ``toctree``, it will cause Sphinx to
error on the build unless you add the ``:orphan:`` role to the top of the page.
We don't want to use this technique for most pages although there are
exceptions.

Rendering Code
==============

Use these tools to render code in your documentation contribution.

Inline Code
-----------

Render inline code by surrounding it with double backticks:

**Example**

.. code-block::

    With the help of a ``with`` block, we can add filters, ordering, and
    pagination clauses.

**Rendered**

With the help of a ``with`` block, we can add filters, ordering, and
pagination clauses.

.. warning::

    Marking up inline code with single backticks a la Markdown will throw an
    error in Sphinx when building the documentation.

Code Blocks
-----------

.. code-block::

    .. code-block:: [<language>]

        <code goes here>

Render a block of code. You can optionally provide a language argument.
Below are the most common languages used in our docs:

* ``bash``- Include the prompt and optionally the output. When a user clicks
  the "copy" button to copy the code, it will copy only the input without the
  prompt and output.

  **Example**

  .. code-block::

      .. code-block:: bash

          $ edgedb configure set listen_addresses 127.0.0.1 ::1

  **Rendered**

  .. code-block:: bash

      $ edgedb configure set listen_addresses 127.0.0.1 ::1

* ``edgeql``- Used for queries.

  **Example**

  .. code-block::

      .. code-block:: edgeql

          select BlogPost filter .id = <uuid>$blog_id;

  **Rendered**

  .. code-block:: edgeql

      select BlogPost filter .id = <uuid>$blog_id;

* ``edgeql-repl``- An alternative to vanilla ``edgeql``. Include the prompt and
  optionally the output. When a user clicks the "copy" button to copy the code,
  it will copy only the input without the prompt and output.

  **Example**

  .. code-block::

      .. code-block:: edgeql-repl

          db> insert Person { name := <str>$name };
          Parameter <str>$name: Pat
          {default::Person {id: e9009b00-8d4e-11ed-a556-c7b5bdd6cf7a}}

  **Rendered**

  .. code-block:: edgeql-repl

      db> insert Person { name := <str>$name };
      Parameter <str>$name: Pat
      {default::Person {id: e9009b00-8d4e-11ed-a556-c7b5bdd6cf7a}}

* ``go``
* ``javascript``
* ``python``

  **Example**

  .. code-block::

      .. code-block:: javascript

          await client.query("select 'I ❤️ ' ++ <str>$name ++ '!';", {
            name: "rock and roll"
          });

  **Rendered**

  .. code-block:: javascript

      await client.query("select 'I ❤️ ' ++ <str>$name ++ '!';", {
        name: "rock and roll"
      });

* ``sdl``- Used for defining schema.

  **Example**

  .. code-block::

      .. code-block:: sdl

          module default {
            type Person {
              required property name -> str { constraint exclusive };
            }
          }

  **Rendered**

  .. code-block:: sdl

      module default {
        type Person {
          required property name -> str { constraint exclusive };
        }
      }

* ``<language>-diff``- Shows changes in a code block. Each line of code in
  these blocks must be prefixed by a character: ``+`` for an added line, ``-``
  for a removed line, or an empty space for an unchanged line.

  **Example**

  .. code-block::

      .. code-block:: sdl-diff

              type Movie {
          -     property title -> str;
          +     required property title -> str;
                multi link actors -> Person;
              }

  **Rendered**

  .. code-block:: sdl-diff

          type Movie {
      -     property title -> str;
      +     required property title -> str;
            multi link actors -> Person;
          }

* No language- Formats the text as a code block but without syntax
  highlighting. Use this for syntaxes that do not offer highlighting or in
  cases where highlighting is unnecessary.

  **Example**

  .. code-block::

      .. code-block::

          [
            {"id": "ea7bad4c-35d6-11ec-9519-0361f8abd380"},
            {"id": "6ddbb04a-3c23-11ec-b81f-7b7516f2a868"},
            {"id": "b233ca98-3c23-11ec-b81f-6ba8c4f0084e"},
          ]

  **Rendered**

  .. code-block::

    [
      {"id": "ea7bad4c-35d6-11ec-9519-0361f8abd380"},
      {"id": "6ddbb04a-3c23-11ec-b81f-7b7516f2a868"},
      {"id": "b233ca98-3c23-11ec-b81f-6ba8c4f0084e"},
    ]

  .. note::

      Code blocks without a language specified do not have a "copy" button.

Code Tabs
---------

``.. tabs::``

Tabs are used to present code examples in multiple languages. This can be
useful when you want to show a query in, for example, both EdgeQL and the
TypeScript query builder.

**Example**

.. code-block::

    .. tabs::

        .. code-tab:: edgeql

            insert Movie {
              title := 'Doctor Strange 2',
              release_year := 2022
            };

        .. code-tab:: typescript

            const query = e.insert(e.Movie, {
              title: 'Doctor Strange 2',
              release_year: 2022
            });

            const result = await query.run(client);

**Rendered**

.. tabs::

    .. code-tab:: edgeql

        insert Movie {
          title := 'Doctor Strange 2',
          release_year := 2022
        };

    .. code-tab:: typescript

        const query = e.insert(e.Movie, {
          title: 'Doctor Strange 2',
          release_year: 2022
        });

        const result = await query.run(client);

.. _ref_guide_contributing_documentation_edgeql:

Documenting EdgeQL
==================

Tools to help document EdgeQL are in the ``:eql:`` domain.

Functions
---------

To document a function use a ``.. eql:function::`` directive. Include these
elements:

* Specify the full function signature with a fully qualified name on the same
  line as the directive.
* Add a description of each parameter using ``:param $<name>: description:``.
  *$<name>* must match the the name of the parameter in function's signature.
  If a parameter is positional rather than named, its number should be used
  instead (e.g. ``$1``).
* Add a type for each parameter using ``:paramtype $<name>: <type>``. For
  example: ``:paramtype $<name>: int64`` declares that the type of the
  *$<name>* parameter is ``int64``. If a parameter has more than one valid
  type, list them separated by "or" like this: ``:paramtype $<name>: int64 or
  str``.
* Document the return value of the function with ``:return:`` and
  ``:returntype:``. ``:return:`` marks a description of the return value and
  ``:returntype:`` its type.
* Finish with a few descriptive paragraphs and code samples. The first
  paragraph must be a single sentence no longer than 79 characters describing
  the function.

**Example**

.. code-block::

    .. eql:function:: std::array_agg(set of any, $a: any) -> array<any>

        :param $1: input set
        :paramtype $1: set of any

        :param $a: description of this param
        :paramtype $a: int64 or str

        :return: array made of input set elements
        :returntype: array<any>

        Return the array made from all of the input set elements.

        The ordering of the input set will be preserved if specified.

You can link to a function's documentation by using the ``:eql:func:`` role.
For instance:

* ``:eql:func:`array_agg```;
* ``:eql:func:`std::array_agg```;

These will link to a function using the function's name as you have written in
between the backticks followed by parentheses. Here are the above links
rendered:

* :eql:func:`array_agg`;
* :eql:func:`std::array_agg`;

You can customize a link's label with this syntax: ``:eql:func:`aggregate a set
as an array <array_agg>```. Here's the rendered output: :eql:func:`aggregate a
set as an array <array_agg>`

Operators
---------

Use the ``.. eql:operator::`` directive to document an operator. On the same
line as the directive, provide a string argument of the format ``<operator-id>:
<operator-signature>``

Add a ``:optype <operand-name>: <type>`` field for each of the operator
signature's operands to declare their types.

**Example**

.. code-block::

    .. eql:operator:: PLUS: A + B

        :optype A: int64 or str or bytes
        :optype B: any
        :resulttype: any

        Arithmetic addition.

You can link to an operator's documentation by using the ``:eql:op:`` role,
followed by the operator's ID you specified in your argument to ``..
eql:operator::``. For instance: ``:eql:op:`plus``` which renders as
:eql:op:`plus`. You can customize the link label like this: ``:eql:op:`+
<plus>```, which renders as :eql:op:`+ <plus>`.

Statements
----------

Use the ``:eql-statement:`` field to sections that describe a statement. Add
the ``:eql-haswith:`` field if the statement supports a :eql:kw:`with` block.

.. code-block::

    Select
    ======

    :eql-statement:
    :eql-haswith:

    ``select``--retrieve or compute a set of values.

    .. eql:synopsis::

        [ with <with-item> [, ...] ]

        select <expr>

        [ filter <filter-expr> ]

        [ order by <order-expr> [direction] [then ...] ]

        [ offset <offset-expr> ]

        [ limit  <limit-expr> ] ;

After laying out the formal syntax, describe the function of each clause with a
synopsis like this:

.. code-block::

    :eql:synopsis:`filter <filter-expr>`
        The optional ``filter`` clause, where :eql:synopsis:`<filter-expr>`
        is any expression that has a result of type :eql:type:`bool`.
        The condition is evaluated for every element in the set produced by
        the ``select`` clause.  The result of the evaluation of the
        ``filter`` clause is a set of boolean values.  If at least one value
        in this set is ``true``, the input element is included, otherwise
        it is eliminated from the output.

These descriptions can each contain as many paragraphs as needed to adequately
describe the clause. Follow the format used in the PostgreSQL documentation.
See `the PostgreSQL SELECT statement reference page
<https://www.postgresql.org/docs/10/static/sql-select.html>`_ for an example.

Use ``:eql:stmt:`select``` to link to the statement's documentation. When
rendered the link looks like this: :eql:stmt:`select`. Customize the label with
``:eql:stmt:`the select statement <select>``` which renders as this:
:eql:stmt:`the select statement <select>`.

Types
-----

To document a type, use the ``.. eql:type::`` directive. Follow the directive
with the fully-qualified name of the type on the same line. The block should
contain the type's description.

.. code-block::

    .. eql:type:: std::bytes

        A sequence of bytes.

To link to a type's documentation, use ``:eql:type:`bytes``` which renders as
:eql:type:`bytes`. You may use the fully qualified name in your reference —
``:eql:type:`std::bytes``` — which renders as :eql:type:`std::bytes`. Both
forms reference the same location in the documentation. Link labels can be
customized with ``:eql:type:`the bytes type <bytes>``` which renders like this:
:eql:type:`the bytes type <bytes>`.

Keywords
--------

Document a keyword using the ``.. eql:keyword::`` directive.

.. code-block::

    .. eql:keyword:: with

        The ``with`` block in EdgeQL is used to define aliases.

If a keyword is compound use a hyphen between each word.

.. code-block::

    .. eql:keyword:: set-of

To link to a keyword's documentation, use the ``:eql:kw:`` role like this:
``:eql:kw:`detached``` which renders as :eql:kw:`detached`. You can customize
the link label like this: ``:eql:kw:`the "detached" keyword <detached>``` which
renders as :eql:kw:`the "detached" keyword <detached>`.

Documenting the EdgeQL CLI
==========================

Document a CLI command using the ``cli:synopsis`` directive like this:

**Example**

.. code-block::

    .. cli:synopsis::

        edgedb dump [<options>] <path>

**Rendered**

.. cli:synopsis::

    edgedb dump [<options>] <path>

The synopsis should follow the format used in the PostgreSQL documentation. See
`the PostgreSQL SELECT statement reference page
<https://www.postgresql.org/docs/10/static/sql-select.html>`_ for an example.

You can then document arguments and options using the ``:cli:synopsis:`` role.

**Example**

.. code-block::

    :cli:synopsis:`<path>`
        The name of the file to backup the database into.

**Rendered**

:cli:synopsis:`<path>`
    The name of the file to backup the database into.


Documentation Versioning
========================

Since EdgeDB functionality is mostly consistent across versions, we offer a
simple method of versioning documentation using two directives.

.. warning::

    Although these are directives included in Sphinx, we have customized them
    to behave differently. Please read this documentation even if you're
    already familiar with the Sphinx directives mentioned here.

New in Version
--------------

Content addressing anything new in a given version are marked with the
``versionadded`` directive. Provide the applicable version as an argument by
placing it just after the directive on the same line.

The directive behaves differently depending on the context.

* When the directive has content (i.e., an indented paragraphs below the
  directive), that content will be shown or hidden based on the version switch.
* When the directive is placed immediately after a section header or inside a
  description block for a function, type, operator, statement, or keyword, that
  entire section or block is marked to be shown or hidden based on the version
  selected.
* When the directive is placed on the top line of any page before any content
  or reStructuredText labels (e.g., ``.. _ref_eql_select:``), it applies to the
  entire page.

**Example with Content**

.. code-block::

    .. versionadded:: 2.0

        This is a new feature that was added in EdgeDB 2.0.

**Rendered**

.. versionadded:: 2.0

    This is a new feature that was added in EdgeDB 2.0.

.. note::

    Change the version in the version selector dropdown to see how the rendered
    example changes.

**Section Example**

.. code-block::

    Source deletion
    ^^^^^^^^^^^^^^^

    .. versionadded:: 2.0

    Source deletion policies determine what action should be taken when the
    *source* of a given link is deleted. They are declared with the ``on source
    delete`` clause.
    ...

**Rendered**

See :ref:`the "Source deletion" section of the "Links" documentation
<ref_datamodel_links_source_deletion>` for a rendered section example of ``..
versionadded:: 2.0``.

**Description Block Example**

.. code-block::

    .. eql:type:: cal::date_duration

        .. versionadded:: 2.0

        A type for representing a span of time in days.

**Rendered**

See :eql:type:`cal::date_duration` for a rendered description block example of
``.. versionadded:: 2.0``.

**Full-Page Example**

.. code-block::

    .. versionadded:: 2.0

    .. _ref_datamodel_globals:

    =======
    Globals
    =======
    ...

**Rendered**

See :ref:`the "Globals" documentation page <ref_datamodel_globals>` for a
full-page example of ``.. versionadded:: 2.0``.

Changed in Version
------------------

Use the ``versionchanged`` directive to mark content related to a change in
existing functionality across EdgeDB versions. Provide the applicable version
as an argument by placing it just after the directive on the same line.

Unlike ``versionadded``, ``versionchanged`` is always used with content to show
or hide that content based on the user's selection in the version dropdown.

**Example**

.. lint-off

.. code-block::

    .. versionchanged:: 3.0

        Starting with the upcoming EdgeDB 3.0, access policy restrictions will
        **not** apply to any access policy expression. This means that when
        reasoning about access policies it is no longer necessary to take other
        policies into account. Instead, all data is visible for the purpose of
        *defining* an access policy.

.. lint-on

**Rendered**

.. versionchanged:: 3.0

    Starting with the upcoming EdgeDB 3.0, access policy restrictions will
    **not** apply to any access policy expression. This means that when
    reasoning about access policies it is no longer necessary to take other
    policies into account. Instead, all data is visible for the purpose of
    *defining* an access policy.

.. note::

    Change the version in the version selector dropdown to see how the rendered
    example changes.

Other Useful Tricks
===================

.. _ref_guide_contributing_documentation_linter_toggle:

Temporarily Disabling Linting
-----------------------------

``.. lint-off`` and ``.. lint-on`` toggle linting off or on. In general,
linting should stay on except in cases where it's impossible to keep it on.
This might be when code or a URL must exceed the maximum line length of 79
characters.

You would typically use this by toggling linting off with ``.. lint-off`` just
before the offending block and back on with ``.. lint-on`` after the block.

**Example**

.. lint-off

.. code-block::

    .. lint-off

    .. code-block::

        GET http://localhost:<port>/branch/main/edgeql?query=insert%20Person%20%7B%20name%20%3A%3D%20%3Cstr%3E$name%20%7D%3B&variables=%7B%22name%22%3A%20%22Pat%22%7D

    .. lint-on

.. lint-on

.. note::

    This is actually a comment our linter pays attention to rather than a
    directive. As a result, it does not end with a colon (``:``) like a
    directive would.

.. note::

    This does not render any visible output.

Embedding a YouTube Video
-------------------------

Embed only videos from `the EdgeDB YouTube channel
<https://www.youtube.com/edgedb>`_

.. code-block::

    .. edb:youtube-embed:: OZ_UURzDkow


Displaying Illustrations
------------------------

Using the ``.. eql:section-intro-page::`` directive, you can display one of
several illustrations. Pass the name of the illustration to the directive by
placing it after the directive on the same line.

**Example**

.. code-block::

    .. eql:section-intro-page:: edgeql

**Rendered**

.. eql:section-intro-page:: edgeql

.. lint-off

See `the list of illustration names
<https://github.com/edgedb/website/blob/master/components/docs/introIllustration/introIllustration.module.scss#L3>`_
and `view the images they map to
<https://github.com/edgedb/website/tree/main/images/doc_illustrations>`_.

.. lint-on
