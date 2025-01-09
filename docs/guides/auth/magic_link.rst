.. _ref_guide_auth_magic_link:

================
Magic Link Auth
================

:edb-alt-title: Integrating EdgeDB Auth's Magic Link provider

Magic Link is a passwordless authentication method that allows users to log in via a unique, time-sensitive link sent to their email. This guide will walk you through integrating Magic Link authentication with your application using EdgeDB Auth.

Enable Magic Link provider
==========================

Before you can use Magic Link authentication, you need to enable the Magic Link provider in your EdgeDB Auth configuration. This can be done through the EdgeDB UI under the "Providers" section.

Magic Link flow
===============

The Magic Link authentication flow involves three main steps:

1. **Sending a Magic Link Email**: Your application requests EdgeDB Auth to send a magic link to the user's email.

2. **User Clicks Magic Link**: The user receives the email and clicks on the magic link.

3. **Authentication and Token Retrieval**: The magic link directs the user to your application, which then authenticates the user and retrieves an authentication token from EdgeDB Auth.

UI considerations
=================

Similar to how the built-in UI works, you can query the database configuration
to discover which providers are configured and dynamically build the UI.

.. code-block:: edgeql

  select cfg::Config.extensions[is ext::auth::AuthConfig].providers {
      name,
      [is ext::auth::OAuthProviderConfig].display_name,
  };

The ``name`` is a unique string that identifies the Identity Provider. OAuth
providers also have a ``display_name`` that you can use as a label for links or
buttons.


Example implementation
======================

We will demonstrate the various steps below by building a NodeJS HTTP server in
a single file that we will use to simulate a typical web application.

.. note::

    The details below show the inner workings of how data is exchanged with the
    Auth extension from a web app using HTTP. You can use this as a guide to
    integrate with your application written in any language that can send and
    receive HTTP requests.


Start the PKCE flow
-------------------

We secure authentication tokens and other sensitive data by using PKCE
(Proof Key of Code Exchange).

Your application server creates a 32-byte Base64 URL-encoded string (which will
be 43 bytes after encoding), called the ``verifier``. You need to store this
value for the duration of the flow. One way to accomplish this bit of state is
to use an HttpOnly cookie when the browser makes a request to the server for
this value, which you can then use to retrieve it from the cookie store at the
end of the flow. Take this ``verifier`` string, hash it with SHA256, and then
base64url encode the resulting string. This new string is called the
``challenge``.

.. lint-off

.. code-block:: javascript

   import http from "node:http";
   import { URL } from "node:url";
   import crypto from "node:crypto";

   /**
    * You can get this value by running `edgedb instance credentials`.
    * Value should be:
    * `${protocol}://${host}:${port}/branch/${branch}/ext/auth/
    */
   const EDGEDB_AUTH_BASE_URL = process.env.EDGEDB_AUTH_BASE_URL;
   const SERVER_PORT = 3000;

   /**
    * Generate a random Base64 url-encoded string, and derive a "challenge"
    * string from that string to use as proof that the request for a token
    * later is made from the same user agent that made the original request
    *
    * @returns {Object} The verifier and challenge strings
    */
   const generatePKCE = () => {
      const verifier = crypto.randomBytes(32).toString("base64url");

      const challenge = crypto
         .createHash("sha256")
         .update(verifier)
         .digest("base64url");

      return { verifier, challenge };
   };

.. lint-on

Routing
-------

Let's set up the routes we will use to handle the magic link authentication
flow. We will then detail each route handler in the following sections.

.. lint-off

.. code-block:: javascript

   const server = http.createServer(async (req, res) => {
     const requestUrl = getRequestUrl(req);

     switch (requestUrl.pathname) {
       case "/auth/magic-link/callback": {
         await handleCallback(req, res);
         break;
       }

       case "/auth/magic-link/signup": {
         await handleSignUp(req, res);
         break;
       }

       case "/auth/magic-link/send": {
         await handleSendMagicLink(req, res);
         break;
       }

       default: {
         res.writeHead(404);
         res.end("Not found");
         break;
       }
     }
   });

.. lint-on

Sign up
-------

.. lint-off

.. code-block:: javascript

   /**
    * Handles sign up with email and password.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleSignUp = async (req, res) => {
     let body = "";
     req.on("data", (chunk) => {
       body += chunk.toString();
     });
     req.on("end", async () => {
       const pkce = generatePKCE();
       const { email, provider } = JSON.parse(body);
       if (!email || !provider) {
         res.status = 400;
         res.end(
           `Request body malformed. Expected JSON body with 'email' and 'provider' keys, but got: ${body}`,
         );
         return;
       }

       const registerUrl = new URL("magic-link/register", EDGEDB_AUTH_BASE_URL);
       const registerResponse = await fetch(registerUrl.href, {
         method: "post",
         headers: {
           "Content-Type": "application/json",
         },
         body: JSON.stringify({
           challenge: pkce.challenge,
           email,
           provider,
           callback_url: `http://localhost:${SERVER_PORT}/auth/magic-link/callback`,
           // The following endpoint will be called if there is an error
           // processing the magic link, such as expiration or malformed token,
           // etc.
           redirect_on_failure: `http://localhost:${SERVER_PORT}/auth_error.html`,
         }),
       });

       if (!registerResponse.ok) {
         const text = await registerResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }

       res.writeHead(204, {
         "Set-Cookie": `edgedb-pkce-verifier=${pkce.verifier}; HttpOnly; Path=/; Secure; SameSite=Strict`,
       });
       res.end();
     });
   };

.. lint-on

Sign in
-------

Signing in with a magic link simply involves telling the EdgeDB Auth server to
send a magic link to the user's email. The user will then click on the link to
authenticate.

.. lint-off

.. code-block:: javascript

   /**
    * Send magic link to existing user's email for sign in.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleSendMagicLink = async (req, res) => {
     let body = "";
     req.on("data", (chunk) => {
       body += chunk.toString();
     });
     req.on("end", async () => {
       const pkce = generatePKCE();
       const { email, provider } = JSON.parse(body);
       if (!email || !provider) {
         res.status = 400;
         res.end(
           `Request body malformed. Expected JSON body with 'email' and 'provider' keys, but got: ${body}`,
         );
         return;
       }

       const emailUrl = new URL("magic-link/email", EDGEDB_AUTH_BASE_URL);
       const authenticateResponse = await fetch(emailUrl.href, {
         method: "post",
         headers: {
           "Content-Type": "application/json",
         },
         body: JSON.stringify({
           challenge: pkce.challenge,
           email,
           provider,
         }),
       });

       if (!authenticateResponse.ok) {
         const text = await authenticateResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
       }

       res.writeHead(204, {
         "Set-Cookie": `edgedb-pkce-verifier=${pkce.verifier}; HttpOnly; Path=/; Secure; SameSite=Strict`,
       });
       res.end();
     });
   };

.. lint-on

Callback
--------

Once the user clicks on the magic link, they will be redirected back to your
application with a ``code`` query parameter. Your application will then exchange
this code for an authentication token.

.. lint-off

.. code-block:: javascript

   /**
    * Handles the PKCE callback and exchanges the `code` and `verifier`
    * for an auth_token, setting the auth_token as an HttpOnly cookie.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleCallback = async (req, res) => {
      const requestUrl = getRequestUrl(req);

      const code = requestUrl.searchParams.get("code");
      if (!code) {
         const error = requestUrl.searchParams.get("error");
         res.status = 400;
         res.end(
            `Magic link callback is missing 'code'. Provider responded with error: ${error}`,
         );
         return;
      }

      const cookies = req.headers.cookie?.split("; ");
      const verifier = cookies
         ?.find((cookie) => cookie.startsWith("edgedb-pkce-verifier="))
         ?.split("=")[1];
      if (!verifier) {
         res.status = 400;
         res.end(
            `Could not find 'verifier' in the cookie store. Is this the same user agent/browser that started the authorization flow?`,
         );
         return;
      }

      const codeExchangeUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
      codeExchangeUrl.searchParams.set("code", code);
      codeExchangeUrl.searchParams.set("verifier", verifier);
      const codeExchangeResponse = await fetch(codeExchangeUrl.href, {
         method: "GET",
      });

      if (!codeExchangeResponse.ok) {
         const text = await codeExchangeResponse.text();
         res.status = 400;
         res.end(`Error from the auth server: ${text}`);
         return;
      }

      const { auth_token } = await codeExchangeResponse.json();
      res.writeHead(204, {
         "Set-Cookie": `edgedb-auth-token=${auth_token}; HttpOnly; Path=/; Secure; SameSite=Strict`,
      });
      res.end();
   };

.. lint-on


Create a User object
--------------------

For some applications, you may want to create a custom ``User`` type in the
default module to attach application-specific information. You can tie this to
an ``ext::auth::Identity`` by using the ``identity_id`` returned during the
sign-up flow.

.. note::

    For this example, we'll assume you have a one-to-one relationship between
    ``User`` objects and ``ext::auth::Identity`` objects. In your own
    application, you may instead decide to have a one-to-many relationship.

Given this ``User`` type:

.. code-block:: sdl

   type User {
       email: str;
       name: str;

       required identity: ext::auth::Identity {
           constraint exclusive;
       };
   }

You can update the ``handleCallback`` function like this to create a new ``User``
object:

.. lint-off

.. code-block:: javascript-diff

     const code = requestUrl.searchParams.get("code");
     if (!code) {
        const error = requestUrl.searchParams.get("error");
        res.status = 400;
        res.end(
           `Magic link callback is missing 'code'. Provider responded with error: ${error}`,
        );
        return;
     }

   + const newIdentityId = requestUrl.searchParams.get("isSignUp") === "true" &&
   +   requestUrl.searchParams.get("identity_id");
   + if (newIdentityId) {
   +   await client.query(`
   +     with
   +       identity := <ext::auth::Identity><uuid>$identity_id,
   +       emailFactor := (
   +         select ext::auth::EmailFactor filter .identity = identity
   +       ),
   +     insert User {
   +       email := emailFactor.email,
   +       identity := identity
   +     };
   +   `, { identity_id: newIdentityId });
   + }
   +
     const cookies = req.headers.cookie?.split("; ");


.. lint-on

:ref:`Back to the EdgeDB Auth guide <ref_guide_auth>`
