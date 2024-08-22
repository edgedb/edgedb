.. _ref_guide_auth_built_in_ui:

===========
Built-in UI
===========

:edb-alt-title: Integrating EdgeDB Auth's built-in UI

To use the built-in UI for EdgeDB Auth, enable the built-in Auth UI by clicking
the "Enable UI" button under "Login UI" in the configuration section of the
EdgeDB UI. Set these configuration values:

-  ``redirect_to``: Once the authentication flow is complete, EdgeDB will
   redirect the user’s browser back to this URL in your application’s
   backend.
-  ``redirect_to_on_signup``: If this is a new user, EdgeDB will redirect
   the user’s browser back to this URL in your application’s backend.
-  ``app_name``: Used in the built-in UI to show the user the
   application’s name in a few important places.
-  ``logo_url``: If provided, will show in the built-in UI as part of the
   page design.
-  ``dark_logo_url``: If provided and the user’s system has indicated
   that they prefer a dark UI, this will show instead of ``logo_url`` in
   the built-in UI as part of the page design.
-  ``brand_color``: If provided, used in the built-in UI as part of the
   page design.


Example Implementation
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

.. note::

   Since ``=`` is not a URL-safe character, if your Base64-URL encoding
   function adds padding, you should remove the padding before hashing the
   ``verifier`` to derive the ``challenge`` or when providing the ``verifier``
   or ``challenge`` in your requests.

.. note::

   If you are familiar with PKCE, you will notice some differences from how RFC
   7636 defines PKCE. Our authentication flow is not an OAuth flow, but rather a
   strict server-to-server flow with Proof Key of Code Exchange added for
   additional security to avoid leaking the authentication token. Here are some
   differences between PKCE as defined in RFC 7636 and our implementation:

   - We do not support the ``plain`` value for ``code_challenge_method``, and
     therefore do not read that value if provided in requests.
   - Our parameters omit the ``code_`` prefix, however we do support
     ``code_challenge`` and ``code_verifier`` as aliases, preferring
     ``challenge`` and ``verifier`` if present.

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


.. note::

    For EdgeDB versions before 5.0, the value for ``EDGEDB_AUTH_BASE_URL``
    in the above snippet should have the form:

    ``${protocol}://${host}:${port}/db/${database}/ext/auth/``


Link to built-in UI
-------------------

Next, provide a link to your web application to either the ``/auth/ui/signin``
or ``auth/ui/signup``. Those routes will generate the ``verifier`` and
``challenge`` strings, save the ``verifier`` in a cookie and redirect the user
to the built-in UI with the ``challenge`` in the search parameters.

.. lint-off

.. code-block:: javascript

   /**
    * In Node, the `req.url` is only the `pathname` portion of a URL. In
    * order to generate a full URL, we need to build the protocol and host
    * from other parts of the request.
    *
    * One reason we like to use `URL` objects here is to easily parse the
    * `URLSearchParams` from the request, and rather than do more error
    * prone string manipulation, we build a `URL`.
    *
    * @param {Request} req
    * @returns {URL}
    */
   const getRequestUrl = (req) => {
      const protocol = req.connection.encrypted ? "https" : "http";
      return new URL(req.url, `${protocol}://${req.headers.host}`);
   };

   const server = http.createServer(async (req, res) => {
      const requestUrl = getRequestUrl(req);

      switch (requestUrl.pathname) {
         case "/auth/ui/signin": {
            await handleUiSignIn(req, res);
            break;
         }

         case "/auth/ui/signup": {
            await handleUiSignUp(req, res);
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
    * Redirects browser requests to EdgeDB Auth UI sign in page with the
    * PKCE challenge, and saves PKCE verifier in an HttpOnly cookie.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleUiSignIn = async (req, res) => {
      const { verifier, challenge } = generatePKCE();

      const redirectUrl = new URL("ui/signin", EDGEDB_AUTH_BASE_URL);
      redirectUrl.searchParams.set("challenge", challenge);

      res.writeHead(301, {
         "Set-Cookie": `edgedb-pkce-verifier=${verifier}; HttpOnly; Path=/; Secure; SameSite=Strict`,
         Location: redirectUrl.href,
      });
      res.end();
   };

   /**
    * Redirects browser requests to EdgeDB Auth UI sign up page with the
    * PKCE challenge, and saves PKCE verifier in an HttpOnly cookie.
    *
    * @param {Request} req
    * @param {Response} res
    */
   const handleUiSignUp = async (req, res) => {
      const { verifier, challenge } = generatePKCE();

      const redirectUrl = new URL("ui/signup", EDGEDB_AUTH_BASE_URL);
      redirectUrl.searchParams.set("challenge", challenge);

      res.writeHead(301, {
         "Set-Cookie": `edgedb-pkce-verifier=${verifier}; HttpOnly; Path=/; Secure; SameSite=Strict`,
         Location: redirectUrl.href,
      });
      res.end();
   };

   server.listen(SERVER_PORT, () => {
      console.log(`HTTP server listening on port ${SERVER_PORT}...`);
   });


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
            `OAuth callback is missing 'code'. \
   OAuth provider responded with error: ${error}`,
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
            `Could not find 'verifier' in the cookie store. Is this the \
   same user agent/browser that started the authorization flow?`,
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
