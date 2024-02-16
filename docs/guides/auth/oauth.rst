.. _ref_guide_auth_oauth:

=====
OAuth
=====

:edb-alt-title: Integrating EdgeDB Auth's OAuth provider

Along with using the `Built-in UI <ref_guide_auth_built_in_ui>`_, you can also
create your own UI that calls to your own web application backend.

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
buttons. In later steps, you'll be providing this ``name`` as the ``provider``
in various endpoints.


Example implementation
======================

We will demonstrate the various steps below by building a NodeJS HTTP server in
a single file that we will use to simulate a typical web application.

.. note::

    We are in the process of publishing helper libraries that you can use with
    popular languages and web frameworks. The details below show the inner
    workings of how data is exchanged with the Auth extension from a web app
    using HTTP. You can use this as a guide to integrate with your application
    written in any language that can send and receive HTTP requests.

We secure authentication tokens and other sensitive data by using PKCE
(Proof Key of Code Exchange).


Start the PKCE flow
-------------------

Your application server creates a 32-byte Base64 URL-encoded string (which will
be 43 bytes after encoding), called the ``verifier``. You need to store this
value for the duration of the flow. One way to accomplish this bit of state is
to use an HttpOnly cookie when the browser makes a request to the server for
this value, which you can then use to retrieve it from the cookie store at the
end of the flow. Take this ``verifier`` string, hash it with SHA256, and then
base64url encode the resulting string. This new string is called the
``challenge``.

.. code-block:: javascript

   import http from "node:http";
   import { URL } from "node:url";
   import crypto from "node:crypto";

   /**
    * You can get this value by running `edgedb instance credentials`.
    * Value should be:
    * `${protocol}://${host}:${port}/db/${database}/ext/auth/
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


Redirect users to Identity Provider
-----------------------------------

Next, we implement a route at ``/auth/authorize`` that the application should
link to when signing in with a particular Identity Provider. We will redirect
the end user's browser to the Identity Provider with the proper setup.

.. lint-off

.. code-block:: javascript

   const server = http.createServer(async (req, res) => {
     const requestUrl = getRequestUrl(req);

     switch (requestUrl.pathname) {
       case "/auth/authorize": {
         await handleAuthorize(req, res);
         break;
       }

       case "/auth/callback": {
         await handleCallback(req, res);
         break;
       }

       default: {
         res.writeHead(404);
         res.end("Not found");
         break;
       }
     }
   });

   /**
    * Redirects OAuth requests to EdgeDB Auth OAuth authorize redirect
    * with the PKCE challenge, and saves PKCE verifier in an HttpOnly
    * cookie for later retrieval.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleAuthorize = async (req, res) => {
     const requestUrl = getRequestUrl(req);
     const provider = requestUrl.searchParams.get("provider");

     if (!provider) {
       res.status = 400;
       res.end("Must provider a 'provider' value in search parameters");
       return;
     }

     const pkce = generatePKCE();
     const redirectUrl = new URL("authorize", EDGEDB_AUTH_BASE_URL);
     redirectUrl.searchParams.set("provider", provider);
     redirectUrl.searchParams.set("challenge", pkce.challenge);
     redirectUrl.searchParams.set(
       "redirect_to",
       `http://localhost:${SERVER_PORT}/auth/callback`,
     );

     res.writeHead(302, {
       "Set-Cookie": `edgedb-pkce-verifier=${pkce.verifier}; HttpOnly; Path=/; Secure; SameSite=Strict`,
       Location: redirectUrl.href,
     });
     res.end();
   };

.. lint-on


Retrieve ``auth_token``
-----------------------

At the very end of the flow, the EdgeDB server will redirect the user's browser
to the ``redirect_to`` address with a single query parameter: ``code``. This
route should be a server route that has access to the ``verifier``. You then
take that ``code`` and look up the ``verifier`` in the ``edgedb-pkce-verifier``
cookie, and make a request to the EdgeDB Auth extension to exchange these two
pieces of data for an ``auth_token``.

.. lint-off

.. code-block:: javascript

   /**
    * Handles the PKCE callback and exchanges the `code` and `verifier
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
            `OAuth callback is missing 'code'. OAuth provider responded with error: ${error}`,
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

:ref:`Back to the EdgeDB Auth guide <ref_guide_auth>`
