.. _ref_guide_auth:

====
Auth
====

:edb-alt-title: Using EdgeDB Auth

EdgeDB Auth is a free batteries-included authentication solution for your app
built on EdgeDB. Here's how you can integrate it with your app.


Enable extension in your schema
===============================

Auth is shipped as an EdgeDB extension, so to enable it, you will need
to add the extension to your app’s schema:

.. code-block:: sdl

    using extension auth;


Extension configuration
=======================

The best and easiest way to configure the extension for your database is
to use the built-in UI. To access it, run ``edgedb ui``. If you have the
extension enabled in your schema as shown above, and if you have migrated that
schema change, you will see the "Auth Admin" icon in the left-hand toolbar.

.. image:: images/ui-auth.png
    :alt: The EdgeDB local development server UI highlighting the auth admin
          icon in the left-hand toolbar. The icon is two nested shield
          outlines, the inner being a light pink color and the outer being
          a light blue when selected.
    :width: 100%

The auth admin UI exposes these values:


``auth_signing_key``
--------------------

The extension uses JSON Web Tokens (JWTs) internally for many operations.
``auth_signing_key`` is the value that is used as a symmetric key for signing
the JWTs. At the moment, the JWTs are not considered “public” API, so there is
no need to save this value for your own application use. It is exposed mainly
to allow rotation.


``token_time_to_live``
----------------------

This value controls the expiration time on the authentication token’s
JSON Web Token. This is effectively the “session” time.


Enabling authentication providers
=================================

In order to use the auth extension, you’ll need to enable at least one of these
authentication providers. Providers can be added from the "Providers" section
of the admin auth UI by clicking "Add Provider." This will add a form to the UI
allowing for selection of the provider and configuration of the values
described below.


Email and password
------------------

-  ``require_verification``: (Default: ``true``) If ``true``, your application
   will not be able to retrieve an authentication token until the user
   has verified their email. If ``false``, your application can retrieve an
   authentication token, but a verification email will still be sent.
   Regardless of this setting, you can always decide to limit access or
   specific features in your application by testing if
   ``ext::auth::EmailPasswordFactor.verified_at`` is set to a date in
   the past on the ``ext::auth::LocalIdentity``.

If you use the Email and Password provider, in addition to the
``require_verification`` configuration, you’ll need to configure SMTP to allow
EdgeDB to send email verification and password reset emails on your behalf.

Here is an example of setting a local SMTP server, in this case using a
product called `Mailpit <https://mailpit.axllent.org/docs/>`__ which is
great for testing in development:

.. code-block:: edgeql

    CONFIGURE CURRENT DATABASE SET
    ext::auth::SMTPConfig::sender := 'hello@example.com';

    CONFIGURE CURRENT DATABASE SET
    ext::auth::SMTPConfig::host := 'localhost';

    CONFIGURE CURRENT DATABASE SET
    ext::auth::SMTPConfig::port := <int32>1025;

    CONFIGURE CURRENT DATABASE SET
    ext::auth::SMTPConfig::username := 'smtpuser';

    CONFIGURE CURRENT DATABASE SET
    ext::auth::SMTPConfig::password := 'smtppassword';

    CONFIGURE CURRENT DATABASE SET
    ext::auth::SMTPConfig::security := 'STARTTLSOrPlainText';

    CONFIGURE CURRENT DATABASE SET
    ext::auth::SMTPConfig::validate_certs := false;


OAuth
-----

We currently support four different OAuth providers (with a few more
coming soon):

-  `Apple <https://developer.apple.com/documentation/sign_in_with_apple/sign_in_with_apple_rest_api/authenticating_users_with_sign_in_with_apple>`__
-  `Azure
   (Microsoft) <https://learn.microsoft.com/en-us/entra/identity-platform/v2-protocols-oidc>`__
-  `GitHub <https://docs.github.com/en/apps/creating-github-apps/registering-a-github-app/registering-a-github-app>`__
-  `Google <https://developers.google.com/identity/protocols/oauth2>`__

The instructions for creating an app for each provider can be found on
each provider’s developer documentation website, which is linked above.
The important things you’ll need to find and make note of for your configuration
are the **client ID** and **secret**.

Once you select the OAuth provider in the configuration UI, you will need to
provide those values and the ``additional_scope``:

-  ``client_id`` This is assigned to you by the Identity Provider when
   you create an app with them.
-  ``secret`` This is created by the Identity Provider when you create
   an app with them.
-  ``additional_scope`` We request certain scope from the Identity
   Provider to fulfill our minimal data needs. You can pass additional
   scope here in a space-separated string and we will request that
   additional scope when getting the authentication token from the
   Identity Provider. \********Note:\*******\* We return this
   authentication token with this scope from the Identity Provider when
   we return our own authentication token.

You’ll also need to set a callback URL in each provider’s interface. To build
this callback URL, you will need the hostname, port, and database name of your
database. The database name is ``edgedb`` by default. The hostname and port can
be found running this CLI command:

.. code-block:: bash

   edgedb instance credentials

This will output a table that includes the hostnames and ports of all your
instances. Grab those from the row corresponding to the correct instance for
use in your callback URL, which takes on this format:

.. code-block::

    http[s]://{edgedb_host}[:port]/db/{db_name}/ext/auth/callback


Integrating your application
============================

In the end, what we want to end up with is an authentication token
created by EdgeDB that we can set as a global in any authenticated
queries executed from our application, which will set a computed global linked
to an ``ext::auth::Identity``.

.. note::

    💡 If you want your own ``User`` type that contains application specific
    information like name, preferences, etc, you can link to this
    ``ext::auth::Identity`` to do so.

You can then use the ``ext::auth::Identity`` (or custom ``User`` type)
to define access policies and make authenticated queries.

Select your method for detailed configuration:

.. toctree::
    :maxdepth: 3

    built_in_ui
    email_password



Build your own UI
-----------------

Email and password
^^^^^^^^^^^^^^^^^^

PKCE
^^^^


OAuth
^^^^^

PKCE
^^^^

We secure authentication tokens and other sensitive data by using PKCE
(Proof Key of Code Exchange).

1. Your application server should create a 32-byte base64 URL encoded
   string (which will be 43-44 bytes after encoding), which we will call
   the ``verifier``. You need to store this value for the duration of
   the flow. One way to accomplish this bit of state is to use an
   HttpOnly cookie when the browser makes a request to the server for
   this value, which you can then use to retrieve it from the cookie
   store at the end of the flow.

2. You take this ``verifier`` string, and then hash it with SHA256, and
   then base64url encode the resulting string. This new string is called
   the ``challenge``.

   .. code-block:: tsx

      import crypto from "node:crypto";

      function initiatePKCE() {
      	const verifier = crypto
          .randomBytes(32)
          .toString("base64url");

      	cookies().set("edgedb_pkce_verifier", verifier);
      	const challenge = crypto
      	  .createHash("sha256")
      	  .update(verifier)
      	  .digest("base64url");

        return challenge;
      }

3. Once you generate the PKCE verifier and challenge you need to link
   the user to the EdgeDB Auth UI API, which will then redirect the
   user’s browser to the configured Identity Provider.

   .. code-block:: tsx

      function getConfiguredProviders() {
        const providers = await client.query<Provider>(`
          select cfg::Config.extensions[is ext::auth::AuthConfig].providers {
            name,
            displayName := [is ext::auth::OAuthProviderConfig].display_name,
          };
        `);
        return providers;
      }

      const challenge = await initiatePKCE();
      const providers = await getConfiguredProviders();
      const redirectTo = new URL(process.env.REDIRECT_TO_URL);
      const makeProviderHref = (name) => {
        const url = new URL("authorize", EDGEDB_AUTH_BASE_URL);
        url.searchParams.set("provider", name);
        url.searchParams.set("challenge", challenge);
        url.searchParams.set("redirect_to", redirectTo);
        return url.href;
      };

      return (
        <>
          {providers.map(({ name, display_name }) => (
            <a href={makeProviderHref(name)}>
              Sign in with {display_name}
            </a>
          )}
        </>
      );

4. Once the flow completes, EdgeDB will redirect the browser to your
   ``redirect_to`` URL with a ``code`` in the query parameter.

5. From the redirect_to route, make a request to the EdgeDB Auth API to
   exchange the ``code`` and ``verifier`` for an ``auth_token`` and
   ``identity_id``.

   .. code-block:: tsx

      const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
      tokenUrl.searchParams.set("code", code);
      tokenUrl.searchParams.set("verifier", verifier);
      const tokenResponse = await fetch(tokenUrl.href, {
        method: "GET",
      });
      if (!tokenResponse.ok) {
        throw new Error("Could not exchange code and verifier for an auth token");
      }
      const { auth_token: authToken, identity_id: identityId } =
        await tokenResponse.json();

      const client = anonymousClient.withGlobals({
        "ext::auth::client_token": authToken,
      });
      await client.query(
        `
        with identity := assert_exists(global ext::auth::ClientTokenIdentity),
        insert User {
          name := "",
          identities := identity,
        };`,
      );


Example usage
=============

Here’s an example schema that we can use to show how you would use the
``auth_token`` you get back from EdgeDB to make queries against a
protected resource, in this case being able to insert a ``Post``.

.. code-block:: tsx

   using extension auth;

   module default {
     global current_user := (
       assert_single((
         select User { id, name }
         filter .identity = global ext::auth::ClientTokenIdentity
       ))
     );

     type User {
       required name: str;
       required identity: ext::auth::Identity;
     }

     type Post {
       required text: str;
       required author: User;

       access policy author_has_full_access
         allow all
         using (.author ?= global current_user);

       access policy others_read_only
         allow select;
     }
   }

Let’s now insert a ``Post``.

.. code-block:: tsx

   const client = createClient().withGlobals({
     "ext::auth::client_token": auth_token,
   });

   const inserted = await client.querySingle(
     `
     insert Post {
       text := <str>$text,
       author := global current_user,
     }`,
     {
       text: 'if your grave doesnt say "rest in peace" on it you are automatically drafted into the skeleton war'
     }
   );

I can even delete it, since I have access through the global:

.. code-block:: tsx

   await client.query(`delete Post filter .id = <str>$id`, { id: inserted.id });
