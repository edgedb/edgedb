.. _ref_guide_auth_email_password:

==================
Email and password
==================

:edb-alt-title: Integrating EdgeDB Auth's email and password provider

Along with using the ``Built-in UI <ref_guide_auth_built_in_ui>``, you can also
create your own UI that calls to your own web application backend.

UI Considerations
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

1. Your application server creates a 32-byte base64 URL encoded string (which
   will be 43 bytes after encoding), called the ``verifier``. You need to store
   this value for the duration of the flow. One way to accomplish this bit of
   state is to use an HttpOnly cookie when the browser makes a request to the
   server for this value, which you can then use to retrieve it from the cookie
   store at the end of the flow. Take this ``verifier`` string, and then hash
   it with SHA256, and then base64url encode the resulting string. This new
   string is called the ``challenge``.

   .. lint-off

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


2. Next, we implement routes that handle registering a new user and
   authenticating an existing user.

   .. code-block:: javascript

      const server = http.createServer(async (req, res) => {
        const requestUrl = getRequestUrl(req);

        switch (requestUrl.pathname) {
          case "/auth/callback": {
            await handleCallback(req, res);
            break;
          }

          case "/auth/signup": {
            await handleSignUp(req, res);
            break;
          }

          case "/auth/signin": {
            await handleSignIn(req, res);
            break;
          }

          case "/auth/verify": {
            await handleVerify(req, res);
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
          const { email, password, provider } = JSON.parse(body);
          if (!email || !password || !provider) {
            res.status = 400;
            res.end(
              `Request body malformed. Expected JSON body with 'email', 'password', and 'provider' keys, but got: ${body}`,
            );
            return;
          }

          const registerUrl = new URL("register", EDGEDB_AUTH_BASE_URL);
          const registerResponse = await fetch(registerUrl.href, {
            method: "post",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              challenge: pkce.challenge,
              email,
              password,
              provider,
              verify_url: `http://localhost:${SERVER_PORT}/auth/verify`,
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

      /**
       * Handles sign in with email and password.
       *
       * @param {Request} req
       * @param {Response} res
       */
      const handleSignIn = async (req, res) => {
        let body = "";
        req.on("data", (chunk) => {
          body += chunk.toString();
        });
        req.on("end", async () => {
          const pkce = generatePKCE();
          const { email, password, provider } = JSON.parse(body);
          if (!email || !password || !provider) {
            res.status = 400;
            res.end(
              `Request body malformed. Expected JSON body with 'email', 'password', and 'provider' keys, but got: ${body}`,
            );
            return;
          }

          const authenticateUrl = new URL("authenticate", EDGEDB_AUTH_BASE_URL);
          const authenticateResponse = await fetch(authenticateUrl.href, {
            method: "post",
            headers: {
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              challenge: pkce.challenge,
              email,
              password,
              provider,
            }),
          });

          if (!authenticateResponse.ok) {
            const text = await authenticateResponse.text();
            res.status = 400;
            res.end(`Error from the auth server: ${text}`);
            return;
          }

          const { code } = await authenticateResponse.json();

          const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
          tokenUrl.searchParams.set("code", code);
          tokenUrl.searchParams.set("verifier", pkce.verifier);
          const tokenResponse = await fetch(tokenUrl.href, {
            method: "get",
          });

          if (!tokenResponse.ok) {
            const text = await authenticateResponse.text();
            res.status = 400;
            res.end(`Error from the auth server: ${text}`);
            return;
          }

          const { auth_token } = await tokenResponse.json();
          res.writeHead(204, {
            "Set-Cookie": `edgedb-auth-token=${auth_token}; HttpOnly; Path=/; Secure; SameSite=Strict`,
          });
          res.end();
        });
      };


   .. lint-on

3. When a new user signs up, by default we require them to verify their email
   address before allowing the application to get an authentication token. To
   handle the verification flow, we implement an endpoint:

   .. lint-off

   .. code-block:: javascript

      /**
       * Handles the link in the email verification flow.
       *
       * @param {Request} req
       * @param {Response} res
       */
      const handleVerify = async (req, res) => {
        const requestUrl = getRequestUrl(req);
        const verification_token = requestUrl.searchParams.get("verification_token");
        if (!verification_token) {
          res.status = 400;
          res.end(
            `Verify request is missing 'verification_token' search param. The verification email is malformed.`,
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

        const verifyUrl = new URL("verify", EDGEDB_AUTH_BASE_URL);
        const verifyResponse = await fetch(verifyUrl.href, {
          method: "post",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            verification_token,
            verifier,
            provider: "builtin::local_emailpassword",
          }),
        });

        if (!verifyResponse.ok) {
          const text = await verifyResponse.text();
          res.status = 400;
          res.end(`Error from the auth server: ${text}`);
          return;
        }

        const { code } = await verifyResponse.json();

        const tokenUrl = new URL("token", EDGEDB_AUTH_BASE_URL);
        tokenUrl.searchParams.set("code", code);
        tokenUrl.searchParams.set("verifier", verifier);
        const tokenResponse = await fetch(tokenUrl.href, {
          method: "get",
        });

        if (!tokenResponse.ok) {
          const text = await tokenResponse.text();
          res.status = 400;
          res.end(`Error from the auth server: ${text}`);
          return;
        }

        const { auth_token } = await tokenResponse.json();
        res.writeHead(204, {
          "Set-Cookie": `edgedb-auth-token=${auth_token}; HttpOnly; Path=/; Secure; SameSite=Strict`,
        });
        res.end();
      };

   .. lint-on

4. Once the request to ``auth/authenticate`` completes, the EdgeDB server
   response with a JSON body with a single property: ``code``.  You take that
   ``code`` and the ``verifier`` you stored in step 1, and make a request to
   the EdgeDB Auth extension to exchange these two pieces of data for an
   ``auth_token``.

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
            "Set-Cookie": `edgedb-auth-token=${auth_token}; Path=/; HttpOnly`,
         });
         res.end();
      };

:ref:`Back to the EdgeDB Auth guide <ref_guide_auth>`
