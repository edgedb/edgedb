.. _ref_guide_using_projects:

=====================
Using EdgeDB Projects
=====================

Projects are the most convenient way to develop applications with EdgeDB. This
is the recommended approach.

To get started, navigate to the root directory of your codebase in a shell and
run ``edgedb project init``. This starts an interactive tool that lets you
configure

.. code-block:: bash

  $ edgedb project init
  No `edgedb.toml` found in current directory or above.
  Do you want to initialize a new project? [Y/n]
  > Y
  Specify the name of EdgeDB instance to use with this project
  [default: my_project]:
  > my_project
  How would you like to run EdgeDB for this project?
  1. Local (native package)
  1. Docker
  Type a number to select an option:
  > 1
  Checking EdgeDB versions...
  Specify the version of EdgeDB to use with this project [default: 1-rc1]:
  > 1-rc1

