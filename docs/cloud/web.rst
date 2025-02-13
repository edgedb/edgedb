.. _ref_guide_cloud_web:

=======
Web GUI
=======

:edb-alt-title: Using EdgeDB Cloud via the web GUI

If you'd prefer, you can also manage your account via `the EdgeDB Cloud
web-based GUI <https://cloud.edgedb.com/>`_.

The first time you access the web UI, you will be prompted to log in. Once you
log in with your account, you'll be on the "Instances" tab of the front page
which shows your instance list. The other two tabs allow you to manage your
organization settings and billing.

Instances
---------

If this is your first time accessing EdgeDB Cloud, this list will be empty. To
create an instance, click "Create new instance." This will pop up a modal
allowing you to name your instance and specify the version of EdgeDB and the
region for the instance.

Once the instance has been created, you'll see the instance dashboard which
allows you to monitor your instance, navigate to the management page for its
branches, and create secret keys.

You'll also see instructions in the bottom-right for linking your EdgeDB CLI to
your EdgeDB Cloud account. You do this by running the CLI command ``edgedb
cloud login``. This will make all of your EdgeDB Cloud instances accessible via
the CLI. You can manage them just as you would other remote EdgeDB instances.

If you want to manage a branch of your database, click through on the
instance's name from the top right of the instance dashboard. If you just
created a database, the branch management view will be mostly empty except
for a button offering to create a sample branch. Once you have a schema
created and some data in a database, this view will offer you similar tools to
those in our local UI.

You'll be able to access a REPL, edit complex queries or build them
graphically, inspect your schema, and browse your data.

Org Settings
------------

This tab allows you to add GitHub organizations for which you are an admin.
If you don't see your organization's name here, you may need to update your
`org settings`_ in GitHub to allow EdgeDB Cloud to read your list of
organizations, and then refresh the org list.

.. lint-off

.. _org setings:
  https://docs.github.com/en/organizations/managing-oauth-access-to-your-organizations-data/approving-oauth-apps-for-your-organization

.. lint-on

Billing
-------

On this page you can manage your account type and payment methods, and set your
email for receiving billing info. Optionally, you can also save your payment
info using `Link <https://link.com/>`_, `Stripe's <https://stripe.com/>`_
fast-checkout solution.
