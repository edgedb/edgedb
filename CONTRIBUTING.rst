How to contribute to EdgeDB
===========================

Thank you for contributing to EdgeDB! We love our open source community and
want to foster a healthy contributor ecosystem.

To make sure the project can continue to improve quickly, we have a few
guidelines designed to make it easier for your contributions to make it into
the project.

These are guidelines rather than hard rules. If you want to submit a pull
request that strays from these, it might be a good idea to start a discussion
about it first. Otherwise, it's possible your pull request might not be merged.

All contributions
-----------------

- **Avoid making pull requests that do not have an associated Github Issue.**
  This could be an already existing issue or one you create yourself when you
  discover the problem. This will allow the team to help you scope your
  solution, warn you of potential gotchas, or give you a heads-up on solutions
  that are likely not feasible. It's a good idea to mention in the issue that
  you'd like to contribute code to resolve the issue.  **If you're fixing
  something trivial like a typo,** an associated issue isn't necessary.
- **Write good commit messages.** The subject of your commit message — that's
  the first line — should tell us *what* you did. The body of your message —
  that's the rest of it — should tell us *why* you did it (unless that's
  self-evident).

Contributing code
--------------------------

- **Pull requests without thorough testing are not likely to be merged.** If
  you're not sure if yours is well-tested enough, go ahead and submit. We can
  help guide you to the finish line.

Contributing documentation
--------------------------

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

Please see EdgeDB's guide for `building documentation
<https://www.edgedb.com/docs/guides/contributing#writing-documentation>`_ from
source.

Documentation style
~~~~~~~~~~~~~~~~~~~

- **Lines should be no longer than 79 characters.**
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
  might easily be interpreted as a reference to EdgeDB's `sets <ref_eql_sets>`.
  Avoid this kind of casual usage of key terms.
