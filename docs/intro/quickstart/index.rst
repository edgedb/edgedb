.. _ref_quickstart:

==========
Quickstart
==========

.. toctree::
  :maxdepth: 1
  :hidden:

  setup
  modeling
  working
  workflow
  access
  inheritance
  dynamic


Welcome to our quickstart tutorial! Together, we'll create a simple HTTP API for a Flashcards application using Next.js and Gel. This practical project will let users build and manage their own study decks, with each flashcard featuring customizable text on both sides - making it perfect for studying, memorization practice, or creating educational games.

Don't worry if you're new to Gel - we'll have you up and running with a Next.js starter project and a local Gel database in just about 5 minutes. From there, we'll guide you through building the complete application in roughly 60-90 minutes.

Our Flashcards app will be a modern web application with the following features:

* Create, edit and delete flashcard decks
* Add and remove cards from decks
* Display cards with front/back text content
* Simple UI with Next.js and Tailwind CSS
* Clean, type-safe data modeling using Gel's schema system

Before you start, you'll need:

* TypeScript, Next.js, and React experience
* Node.js 20+
* Unix-like OS (Linux, macOS, or WSL)
* Your preferred code editor

Why Gel for Next.js?
--------------------

This tutorial will show you how Gel enhances your Next.js development workflow by providing a robust data layer that feels natural in a TypeScript environment. Here's why Gel is an ideal choice:

* **Type Safety**: Gel's strict type system catches data errors before runtime
* **Rich Data Modeling**: Object types and links make it natural to model related data
* **Modern Tooling**: First-class TypeScript support and migrations management
* **Performance**: Efficient query execution for complex data relationships
* **Developer Experience**: Clean query language that's more intuitive than raw SQL

You'll learn how Gel's schema system lets you model your data intuitively - a refreshing alternative to mapping between SQL tables and TypeScript types. As we build the application, you'll discover how Gel's query language, EdgeQL, makes complex data operations straightforward and type-safe. We'll also explore how to evolve your schema over time using Gel's migration system.

Need Help?
----------

If you run into issues while following this tutorial:

* Check the `Gel documentation <https://docs.geldata.com>`_
* Visit our `community Discord <https://discord.gg/gel>`_
* File an issue on `GitHub <https://github.com/geldata/gel>`_
