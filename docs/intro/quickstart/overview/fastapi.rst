.. _ref_quickstart_fastapi:

==========
Quickstart
==========

Welcome to the quickstart tutorial! In this tutorial, you will update a FastAPI
backend for a Flashcards application to use |Gel| as your data layer. The
application will let users build and manage their own study decks, with each
flashcard featuring customizable text on both sides - making it perfect for
studying, memorization practice, or creating educational games.

Don't worry if you're new to |Gel| - you will be up and running with a working
FastAPI backend and a local |Gel| database in just about **5 minutes**. From
there, you will replace the static mock data with a |Gel| powered data layer in
roughly 30-45 minutes.

By the end of this tutorial, you will be comfortable with:

*  Creating and updating a database schema
*  Running migrations to evolve your data
*  Writing EdgeQL queries
*  Building an app backed by |Gel|


Features of the flashcards app
------------------------------

*  Create, edit, and delete decks
*  Add/remove cards with front/back content
*  Clean, type-safe schema with |Gel|

Requirements
------------

Before you start, you need:

*  Basic familiarity with Python and FastAPI
*  Python 3.8+ on a Unix-like OS (Linux, macOS, or WSL)
*  A code editor you love

Why |Gel| for FastAPI?
----------------------

*  **Type Safety**: Catch data errors before runtime
*  **Rich Modeling**: Use object types and links to model relations
*  **Modern Tooling**: Python-friendly schemas and migrations
*  **Performance**: Efficient queries for complex data
*  **Developer Experience**: An intuitive query language (EdgeQL)

Need Help?
----------

If you run into issues while following this tutorial:

-  Check the `Gel documentation <https://docs.geldata.com>`_
-  Visit our `community Discord <https://discord.gg/gel>`_
-  File an issue on `GitHub <https://github.com/geldata/gel>`_
